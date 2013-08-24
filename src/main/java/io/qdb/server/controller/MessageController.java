/*
 * Copyright 2013 David Tinker
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.qdb.server.controller;

import io.qdb.buffer.MessageBuffer;
import io.qdb.buffer.MessageCursor;
import io.qdb.server.databind.DateTimeParser;
import io.qdb.server.filter.MessageFilter;
import io.qdb.server.filter.MessageFilterFactory;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Singleton
public class MessageController extends CrudController {

    private static final Logger log = LoggerFactory.getLogger(MessageController.class);

    private final QueueManager queueManager;
    private final MessageFilterFactory messageFilterFactory;

    public static class CreateDTO {

        public long id;
        public Date timestamp;
        public int payloadSize;
        public String routingKey;

        public CreateDTO(long id, Date timestamp, int payloadSize, String routingKey) {
            this.id = id;
            this.timestamp = timestamp;
            this.payloadSize = payloadSize;
            this.routingKey = routingKey;
        }
    }

    public static class MessageHeader {

        public long id;
        public Date timestamp;
        public int payloadSize;
        public String routingKey;

        @SuppressWarnings("UnusedDeclaration")
        public MessageHeader() { }

        public MessageHeader(MessageCursor c, long id, long timestamp, String routingKey, byte[] payload) throws IOException {
            this.id = id;
            this.timestamp = new Date(timestamp);
            this.routingKey = routingKey;
            payloadSize = payload == null ? c.getPayloadSize() : payload.length;
        }
    }

    @Inject
    public MessageController(JsonService jsonService, QueueManager queueManager,
                             MessageFilterFactory messageFilterFactory) {
        super(jsonService);
        this.queueManager = queueManager;
        this.messageFilterFactory = messageFilterFactory;
    }

    @Override
    protected void create(Call call) throws IOException {
        MessageBuffer mb = queueManager.getBuffer(call.getQueue());
        if (mb == null || !mb.isOpen()) {
            // probably we are busy starting up and haven't synced this queue yet or are shutting down
            call.setCode(503, "Queue is not available, please try again later");
            return;
        }

        if (call.getBoolean("multiple")) {
            createMultiple(call, mb);
        } else {
            createSingle(call, mb);
        }
    }

    private void createSingle(Call call, MessageBuffer mb) throws IOException {
        Request request = call.getRequest();
        String routingKey = request.getParameter("routingKey");
        int contentLength = request.getContentLength();
        long id = 0;
        long timestamp = System.currentTimeMillis();
        IllegalArgumentException err = null;
        try {
            if (contentLength < 0) {
                byte[] payload = readAll(request.getInputStream());
                contentLength = payload.length;
                id = mb.append(timestamp, routingKey, payload);
            } else {
                ReadableByteChannel in = request.getByteChannel();
                try {
                    id = mb.append(timestamp, routingKey, in, contentLength);
                } finally {
                    close(in);
                }
            }
        } catch (IllegalArgumentException e) {
            err = e;
        }

        if (err != null) {
            call.setCode(422, err.getMessage());
        } else {
            call.setCode(201, new CreateDTO(id, new Date(timestamp), contentLength, routingKey));
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

    private void createMultiple(Call call, MessageBuffer mb) throws IOException {
        int maxPayloadSize = mb.getMaxPayloadSize();
        InputStream in = call.getRequest().getInputStream();
        List<CreateDTO> created = new ArrayList<CreateDTO>();
        try {
            for (;;) {
                String routingKey;
                byte[] data = nextNetstring(in, 1024, "routing key");
                if (data == null) break;
                routingKey = new String(data, "UTF8");

                data = nextNetstring(in, maxPayloadSize, "payload");
                if (data == null) {
                    throw new IllegalArgumentException("Expected payload for message with routing key [" +
                            routingKey + "]");
                }

                long timestamp = System.currentTimeMillis();
                created.add(new CreateDTO(mb.append(timestamp, routingKey, data), new Date(timestamp), data.length,
                        routingKey));
            }
        } catch (IllegalArgumentException e) {
            call.setCode(422, new MultipleErrorDTO(422, e.getMessage(), created.isEmpty() ? null : created));
            return;
        } finally {
            close(in);
        }

        call.setCode(created.size() > 0 ? 201 : 200, created);
    }

    public static class MultipleErrorDTO extends Renderer.StatusMsg {
        public List<CreateDTO> created;
        public MultipleErrorDTO(int responseCode, String message, List<CreateDTO> created) {
            super(responseCode, message);
            this.created = created;
        }
    }

    /**
     * Read newline terminated nestring from in, returning null on EOF.
     * See http://en.wikipedia.org/wiki/Netstrings. Throws IllegalArgumentException on invalid input.
     */
    private byte[] nextNetstring(InputStream in, int maxSize, String item) throws IOException, IllegalArgumentException {
        int b;
        for (;;) {
            b = in.read();
            if (b == -1) return null;
            if (b != '\n' && b != '\r') break;
        }

        int len = toDigit(b);
        for (;;) {
            b = in.read();
            if (b == ':') break;
            len = len * 10 + toDigit(b);
            if (len < 0) {  // overflow
                throw new IllegalArgumentException("Invalid length found while reading " + item);
            }
        }
        if (len > maxSize) {
            throw new IllegalArgumentException("Length " + len + " exceeds max " + maxSize +
                    " while reading " + item);
        }

        byte[] data = new byte[len];
        int todo = len;
        for (; todo > 0; ) {
            int sz = in.read(data, len - todo, todo);
            if (sz < 0) {
                throw new IllegalArgumentException("Expected " + len + " bytes, only read " + (len - todo) +
                        " while reading " + item);
            }
            todo -= sz;
        }
        return data;
    }

    private int toDigit(int b) {
        int ans = b - '0';
        if (ans < 0 || ans > 9) {
            throw new IllegalArgumentException("Expected '0'-'9', got '" + (char)b + "' (" + b +
                    ", 0x" + Integer.toHexString(b) + ") ");
        }
        return ans;
    }

    private void close(Closeable c) {
        try {
            if (c != null) c.close();
        } catch (IOException e) {
            log.warn("Error closing " + c + ": " + e);
        }
    }

    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        Queue q = call.getQueue();
        MessageBuffer mb = queueManager.getBuffer(q);
        if (mb == null || !mb.isOpen()) {
            // probably we are busy starting up and haven't synced this queue yet or are shutting down
            call.setCode(503, "Queue is not available, please try again later");
            return;
        }

        MessageFilter mf;
        try {
            mf = messageFilterFactory.createFilter(call.getRequest().getQuery(), q);
        } catch (IllegalArgumentException e) {
            call.setCode(422, e.getMessage());
            return;
        }

        int timeoutMs = call.getInt("timeoutMs", 0);
        byte[] keepAlive = call.getUTF8Bytes("keepAlive", "\n");
        int keepAliveMs = call.getInt("keepAliveMs", 29000);
        byte[] separator = call.getUTF8Bytes("separator", "\n");
        boolean noHeaders = call.getBoolean("noHeaders");
        boolean noPayload = call.getBoolean("noPayload");
        boolean noLengthPrefix = call.getBoolean("noLengthPrefix");
        boolean borg = call.getBoolean("borg");

        boolean single = call.getBoolean("single");
        if (single) {
            limit = 1;
            keepAliveMs = Integer.MAX_VALUE;
        }

        Date from = call.getDate("from");
        long fromId = from != null ? -1 : call.getLong("fromId", mb.getNextId());

        long to = call.getTimestamp("to");
        long toId = to > 0 ? -1 : call.getLong("toId", -1);

        Response response = call.getResponse();
        response.set("Content-Type", single ? q.getContentType() : "application/octet-stream");
        OutputStream out = response.getOutputStream();

        MessageCursor c = from != null ? mb.cursorByTimestamp(from.getTime()) : mb.cursor(fromId);

        int nextKeepAliveMs = keepAliveMs;
        for (int sent = 0; limit == 0 || sent < limit; ) {
            try {
                if (timeoutMs <= 0) {
                    while (!c.next(single ? 0 : nextKeepAliveMs)) {
                        out.write(keepAlive);
                        out.flush();
                        nextKeepAliveMs = keepAliveMs;
                    }
                } else {
                    int ms = timeoutMs;
                    while (true) {
                        int waitMs = Math.min(ms, nextKeepAliveMs);
                        if (c.next(waitMs)) break;
                        if ((ms -= waitMs) <= 0) break;
                        out.write(keepAlive);
                        out.flush();
                        nextKeepAliveMs = keepAliveMs;
                    }
                    if (ms <= 0) break;
                }
            } catch (InterruptedException e) {
                break;
            }

            if (to > 0 && c.getTimestamp() >= to || toId > 0 && c.getId() >= toId) break;

            long id = c.getId();
            long timestamp = c.getTimestamp();
            String routingKey = c.getRoutingKey();
            byte[] payload = null;
            MessageFilter.Result result = mf.accept(id, timestamp, routingKey, null);
            if (result == MessageFilter.Result.CHECK_PAYLOAD) {
                result = mf.accept(id, timestamp, routingKey, payload = c.getPayload());
            }

            if (result == MessageFilter.Result.ACCEPT) {
                if (single) {
                    response.setContentLength(noPayload ? 0 : c.getPayloadSize());
                    response.set("QDB-Id", Long.toString(c.getId()));
                    response.set("QDB-Timestamp", borg
                            ? Long.toString(timestamp)
                            : DateTimeParser.INSTANCE.formatTimestamp(new Date(timestamp)));
                    response.set("QDB-RoutingKey", routingKey);
                    if (!noPayload) out.write(payload == null ? c.getPayload() : payload);
                } else {
                    if (!noHeaders) {
                        MessageHeader h = new MessageHeader(c, id, timestamp, routingKey, payload);
                        byte[] data = jsonService.toJsonMsgHeader(h, borg);
                        if (!noLengthPrefix) out.write((data.length + ":").getBytes("UTF8"));
                        out.write(data);
                        out.write(10);
                    }
                    if (!noPayload) {
                        out.write(payload == null ? c.getPayload() : payload);
                        out.write(separator);
                    }
                    nextKeepAliveMs = 100;
                }
                ++sent;
            }
        }
    }
}
