package io.qdb.server.controller;

import io.qdb.buffer.MessageBuffer;
import io.qdb.buffer.MessageCursor;
import io.qdb.server.OurServer;
import io.qdb.server.model.Queue;
import io.qdb.server.queue.QueueManager;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.*;
import java.nio.channels.ReadableByteChannel;

@Singleton
public class MessageController extends CrudController {

    private static final Logger log = LoggerFactory.getLogger(MessageController.class);

    private final QueueManager queueManager;
    private final String ourServerId;

    public static class CreateDTO {

        public long id;
        public long timestamp;
        public int payloadSize;

        public CreateDTO(long id, long timestamp, int payloadSize) {
            this.id = id;
            this.timestamp = timestamp;
            this.payloadSize = payloadSize;
        }
    }

    public static class MessageHeader {

        public long id;
        public long timestamp;
        public int payloadSize;
        public String routingKey;

        public MessageHeader(MessageCursor c) throws IOException {
            id = c.getId();
            timestamp = c.getTimestamp();
            payloadSize = c.getPayloadSize();
            routingKey = c.getRoutingKey();
        }
    }

    @Inject
    public MessageController(JsonService jsonService, QueueManager queueManager, OurServer ourServer) {
        super(jsonService);
        this.queueManager = queueManager;
        this.ourServerId = ourServer.getId();
    }

    @Override
    protected void create(Call call) throws IOException {
        Queue q = call.getQueue();
        if (!q.isMaster(ourServerId)) {
            // todo proxy POST to master and set Location header
            call.setCode(500, "Create message on non-master not implemented");
            return;
        }
        MessageBuffer mb = queueManager.getBuffer(q);
        if (mb == null || !mb.isOpen()) {
            // probably we are busy starting up and haven't synced this queue yet or are shutting down
            call.setCode(503, "Queue is not available, please try again later");
            return;
        }

        Request request = call.getRequest();
        String routingKey = request.getParameter("routingKey");
        int contentLength = request.getContentLength();
        long id = 0;
        long timestamp = System.currentTimeMillis();
        IllegalArgumentException err = null;
        if (contentLength < 0) {
            byte[] payload = readAll(request.getInputStream());
            contentLength = payload.length;
            try {
                id = mb.append(timestamp, routingKey, payload);
            } catch (IllegalArgumentException e) {
                err = e;
            }
        } else {
            ReadableByteChannel in = request.getByteChannel();
            try {
                id = mb.append(timestamp, routingKey, in, contentLength);
            } catch (IllegalArgumentException e) {
                err = e;
            } finally {
                close(in);
            }
        }

        if (err != null) {
            call.setCode(400, err.getMessage());
        } else {
            call.setCode(201, new CreateDTO(id, timestamp, contentLength));
        }
    }

    private void close(Closeable c) {
        try {
            if (c != null) c.close();
        } catch (IOException e) {
            log.warn("Error closing " + c + ": " + e);
        }
    }

    private byte[] readAll(InputStream in) throws IOException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(16384);
            byte[] buf = new byte[16384];
            for (;;) {
                int sz = in.read(buf, 0, buf.length);
                if (sz < 0) break;
                bos.write(buf, 0, sz);
            }
            return bos.toByteArray();
        } finally {
            close(in);
        }
    }

    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        Queue q = call.getQueue();
        if (!q.isMaster(ourServerId) && !q.isSlave(ourServerId)) {
            // todo set Location header and send a 302 or proxy the master
            call.setCode(500, "Get messages from non-master non-slave not implemented");
            return;
        }
        MessageBuffer mb = queueManager.getBuffer(q);
        if (mb == null || !mb.isOpen()) {
            // probably we are busy starting up and haven't synced this queue yet or are shutting down
            call.setCode(503, "Queue is not available, please try again later");
            return;
        }

        int timeoutMs = call.getInt("timeoutMs", 0);
        byte[] keepAlive = call.getUTF8Bytes("keepAlive", "\n");
        int keepAliveMs = call.getInt("keepAliveMs", 29000);
        byte[] separator = call.getUTF8Bytes("separator", "\n");

        boolean single = call.getBoolean("single");
        if (single) {
            limit = 1;
            keepAliveMs = Integer.MAX_VALUE;
        }

        long id = call.getLong("id", -1L);
        long timestamp = -1;
        if (id < 0) {
            timestamp = call.getLong("timestamp", -1L);
            if (timestamp < 0) id = mb.getNextMessageId();
        }

        Response response = call.getResponse();
        response.set("Content-Type", single ? q.getContentType() : "text/plain");
        OutputStream out = response.getOutputStream();

        MessageCursor c = id < 0 ? mb.cursorByTimestamp(timestamp) : mb.cursor(id);

        for (int sent = 0; limit == 0 || sent < limit; ++sent) {
            try {
                if (timeoutMs <= 0) {
                    while (!c.next(single ? 0 : keepAliveMs)) {
                        out.write(keepAlive);
                        out.flush();
                    }
                } else {
                    int ms = timeoutMs;
                    while (true) {
                        int waitMs = Math.min(ms, keepAliveMs);
                        if (c.next(waitMs)) break;
                        if ((ms -= waitMs) <= 0) break;
                        out.write(keepAlive);
                        out.flush();
                    }
                    if (ms <= 0) break;
                }
            } catch (InterruptedException e) {
                break;
            }

            if (single) {
                response.setContentLength(c.getPayloadSize());
                response.set("X-QDB-Id", Long.toString(c.getId()));
                response.set("X-QBD-Timestamp", Long.toString(c.getTimestamp()));
                response.set("X-QDB-RoutingKey", c.getRoutingKey());
                out.write(c.getPayload());
            } else {
                out.write(jsonService.toJsonNoIndenting(new MessageHeader(c)));
                out.write(10);
                out.write(c.getPayload());
                out.write(separator);
            }
        }
    }

}
