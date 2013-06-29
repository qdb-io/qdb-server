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
import io.qdb.server.model.Queue;
import io.qdb.server.queue.QueueManager;
import org.omg.CORBA.DynAnyPackage.Invalid;
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

    public static class CreateDTO {

        public long id;
        public Date timestamp;
        public int payloadSize;

        public CreateDTO(long id, Date timestamp, int payloadSize) {
            this.id = id;
            this.timestamp = timestamp;
            this.payloadSize = payloadSize;
        }
    }

    public static class MessageHeader {

        public long id;
        public Date timestamp;
        public int payloadSize;
        public String routingKey;

        @SuppressWarnings("UnusedDeclaration")
        public MessageHeader() { }

        public MessageHeader(MessageCursor c) throws IOException {
            id = c.getId();
            timestamp = new Date(c.getTimestamp());
            payloadSize = c.getPayloadSize();
            routingKey = c.getRoutingKey();
        }
    }

    @Inject
    public MessageController(JsonService jsonService, QueueManager queueManager) {
        super(jsonService);
        this.queueManager = queueManager;
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
        if (contentLength < 0) {
            byte[] payload = readAll(request.getInputStream());
            contentLength = payload.length;
            try {
                id = mb.append(timestamp, routingKey, payload);
            } catch (IllegalArgumentException e) {
                log.debug(e.toString(), e);
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
            call.setCode(422, err.getMessage());
        } else {
            call.setCode(201, new CreateDTO(id, new Date(timestamp), contentLength));
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
                byte[] data = nextNetstring(in, 1024 * 1024, "routing key");
                if (data == null) break;
                routingKey = new String(data, "UTF8");

                data = nextNetstring(in, maxPayloadSize, "payload");
                if (data == null) {
                    throw new IllegalArgumentException("Expected payload for message with routing key [" +
                            routingKey + "]");
                }

                long timestamp = System.currentTimeMillis();
                created.add(new CreateDTO(mb.append(timestamp, routingKey, data), new Date(timestamp), data.length));
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
        }
        if (len < 0) {
            throw new IllegalArgumentException("Invalid length " + len + " while reading " + item);
        }
        if (len > maxSize) {
            throw new IllegalArgumentException("Length " + len + " exceeds maxPayloadSize " + maxSize +
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

        int timeoutMs = call.getInt("timeoutMs", 0);
        byte[] keepAlive = call.getUTF8Bytes("keepAlive", "\n");
        int keepAliveMs = call.getInt("keepAliveMs", 29000);
        byte[] separator = call.getUTF8Bytes("separator", "\n");
        boolean noHeaders = call.getBoolean("noHeaders");

        boolean single = call.getBoolean("single");
        if (single) {
            limit = 1;
            keepAliveMs = Integer.MAX_VALUE;
        }

        Date at = call.getDate("at");
        long id = at != null ? -1 : call.getLong("id", mb.getNextMessageId());

        Response response = call.getResponse();
        response.set("Content-Type", single ? q.getContentType() : "text/plain");
        OutputStream out = response.getOutputStream();

        MessageCursor c = at != null ? mb.cursorByTimestamp(at.getTime()) : mb.cursor(id);

        int nextKeepAliveMs = keepAliveMs;
        for (int sent = 0; limit == 0 || sent < limit; ++sent) {
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

            if (single) {
                response.setContentLength(c.getPayloadSize());
                response.set("X-QDB-Id", Long.toString(c.getId()));
                response.set("X-QBD-Timestamp", DateTimeParser.INSTANCE.formatTimestamp(new Date(c.getTimestamp())));
                response.set("X-QDB-RoutingKey", c.getRoutingKey());
                out.write(c.getPayload());
            } else {
                if (!noHeaders) {
                    out.write(jsonService.toJsonNoIndenting(new MessageHeader(c)));
                    out.write(10);
                }
                out.write(c.getPayload());
                out.write(separator);
                nextKeepAliveMs = 100;
            }
        }
    }

}
