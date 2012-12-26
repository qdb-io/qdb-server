package io.qdb.server.controller;

import io.qdb.buffer.MessageBuffer;
import io.qdb.server.JsonService;
import io.qdb.server.ServerId;
import io.qdb.server.model.Queue;
import io.qdb.server.model.Repository;
import io.qdb.server.queue.QueueManager;
import org.simpleframework.http.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ReadableByteChannel;

@Singleton
public class MessageController extends CrudController {

    private static final Logger log = LoggerFactory.getLogger(MessageController.class);

    private final Repository repo;
    private final QueueManager queueManager;
    private final String serverId;

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

    @Inject
    public MessageController(JsonService jsonService, Repository repo, QueueManager queueManager, ServerId serverId) {
        super(jsonService);
        this.repo = repo;
        this.queueManager = queueManager;
        this.serverId = serverId.get();
    }

    @Override
    protected void create(Call call) throws IOException {
        Queue q = call.getQueue();
        if (!q.isMaster(serverId)) {
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
}
