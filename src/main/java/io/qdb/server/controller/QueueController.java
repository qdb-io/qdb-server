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
import io.qdb.server.databind.DurationParser;
import io.qdb.server.model.*;
import io.qdb.server.model.Queue;
import io.qdb.server.queue.QueueManager;
import io.qdb.server.queue.QueueStatusMonitor;
import io.qdb.server.monitor.Status;
import io.qdb.server.repo.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;
import java.util.regex.Pattern;

@Singleton
public class QueueController extends CrudController {

    private final Repository repo;
    private final MessageController messageController;
    private final TimelineController timelineController;
    private final OutputController outputController;
    private final QueueManager queueManager;
    private final QueueStatusMonitor queueStatusMonitor;

    private static final SecureRandom RND = new SecureRandom();

    private static final Logger log = LoggerFactory.getLogger(QueueController.class);

    public static class QueueDTO implements Comparable<QueueDTO> {

        public String id;
        public Integer version;
        public String database;
        public Long maxSize;
        public Integer maxPayloadSize;
        public String contentType;
        public Object warnAfter;
        public Object errorAfter;

        public String status;
        public Long size;
        public Long messageCount;
        public Object duration;
        public Date oldestMessage;
        public Date newestMessage;
        public Object newestMessageReceived;
        public Long oldestMessageId;
        public Long nextMessageId;

        @SuppressWarnings("UnusedDeclaration")
        public QueueDTO() { }

        public QueueDTO(String id, Queue queue, boolean borg) {
            this.id = id;
            this.version = queue.getVersion();
            this.maxSize = queue.getMaxSize();
            this.maxPayloadSize = queue.getMaxPayloadSize();
            this.contentType = queue.getContentType();
            if (borg) {
                this.warnAfter = null0(queue.getWarnAfter());
                this.errorAfter = null0(queue.getErrorAfter());
            } else {
                int secs = queue.getWarnAfter();
                if (secs > 0) this.warnAfter = DurationParser.formatHumanMs(secs * 1000L);
                secs = queue.getErrorAfter();
                if (secs > 0) this.errorAfter = DurationParser.formatHumanMs(secs * 1000L);
            }
        }

        private Integer null0(int x) {
            return x == 0 ? null : x;
        }

        @Override
        public int compareTo(QueueDTO o) {
            return id.compareTo(o.id);
        }
    }

    private static final Pattern VALID_QUEUE_ID = Pattern.compile("[0-9a-z\\-_]+", Pattern.CASE_INSENSITIVE);

    @Inject
    public QueueController(JsonService jsonService, Repository repo, MessageController messageController,
                           TimelineController timelineController, OutputController outputController,
                           QueueManager queueManager, QueueStatusMonitor queueStatusMonitor) {
        super(jsonService);
        this.repo = repo;
        this.messageController = messageController;
        this.timelineController = timelineController;
        this.outputController = outputController;
        this.queueManager = queueManager;
        this.queueStatusMonitor = queueStatusMonitor;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        List<QueueDTO> ans = new ArrayList<QueueDTO>();
        Map<String, String> queues = call.getDatabase().getQueues();
        if (queues != null) {
            for (Map.Entry<String, String> e : queues.entrySet()) {
                Queue queue = repo.findQueue(e.getValue());
                if (queue != null) ans.add(createQueueDTO(call, e.getKey(), queue));
            }
            Collections.sort(ans);
            int last = Math.min(offset + limit, ans.size());
            if (offset > 0 || last < ans.size()) {
                if (offset >= ans.size()) ans = Collections.EMPTY_LIST;
                else ans = ans.subList(offset, last);
            }
        }
        call.setJson(ans);
    }

    @Override
    protected void count(Call call) throws IOException {
        Map<String, String> queues = call.getDatabase().getQueues();
        call.setJson(new Count(queues == null ? 0 : queues.size()));
    }

    @Override
    protected void show(Call call, String id) throws IOException {
        Map<String, String> queues = call.getDatabase().getQueues();
        if (queues != null) {
            String queueId = queues.get(id);
            if (queueId != null) {
                Queue queue = repo.findQueue(queueId);
                if (queue != null) {
                    call.setJson(createQueueDTO(call, id, queue));
                    return;
                }
            }
        }
        call.setCode(404);
    }

    protected QueueDTO createQueueDTO(Call call, String id, Queue queue) throws IOException {
        boolean borg = call.getBoolean("borg");
        QueueDTO dto = new QueueDTO(id, queue, borg);
        try {
            MessageBuffer mb = queueManager.getBuffer(queue);
            if (mb != null) {
                dto.size = mb.getSize();
                dto.messageCount = mb.getMessageCount();
                dto.oldestMessage = mb.getOldestTimestamp();
                dto.oldestMessageId = mb.getOldestId();
                dto.newestMessage = mb.getMostRecentTimestamp();
                if (dto.newestMessage != null) {
                    long ms = System.currentTimeMillis() - dto.newestMessage.getTime();
                    dto.newestMessageReceived = borg ? ms : DurationParser.formatHumanMs(ms) + " ago";
                    ms = dto.newestMessage.getTime() - dto.oldestMessage.getTime();
                    dto.duration = borg ? ms : DurationParser.formatHumanMs(ms);
                }
                dto.nextMessageId = mb.getNextId();
            }

            Status status = queueStatusMonitor.getStatus(queue);
            if (status != null) dto.status = status.toString();
        } catch (IOException e) {
            log.error("/db/" + queue.getDatabase() + "/q/" + id + ": " + e, e);
        }
        return dto;
    }

    @Override
    protected void createOrUpdate(Call call, String id) throws IOException {
        QueueDTO dto = getBodyObject(call, QueueDTO.class);
        boolean create;
        Queue q;
        synchronized (repo) {
            // re-lookup db inside sync block in case we need to update it
            Database db = repo.findDatabase(call.getDatabase().getId());
            if (db == null) {   // this isn't likely but isn't impossible either
                call.setCode(404);
                return;
            }

            String qid = db.getQidForQueue(id);
            if (create = qid == null) {
                if (call.isPut()) {
                    call.setCode(404);
                    return;
                }
                if (!VALID_QUEUE_ID.matcher(id).matches()) {
                    call.setCode(422, "Queue id must contain only letters, numbers, hyphens and underscores");
                    return;
                }

                q = new Queue();
                q.setDatabase(db.getId());
                q.setMaxSize(100 * 1024 * 1024);
                q.setMaxPayloadSize(1024 * 1024);
                q.setContentType("application/octet-stream");
            } else {
                q = repo.findQueue(qid);
                if (q == null) {    // this shouldn't happen
                    String msg = "Queue /db/" + db.getId() + "/q/" + id + " qid [" + qid +
                            "] not found";
                    log.error(msg);
                    call.setCode(500, msg);
                    return;
                }
                if (dto.version != null && !dto.version.equals(q.getVersion())) {
                    call.setCode(409, createQueueDTO(call, id, q));
                    return;
                }
                q = q.deepCopy();
            }

            boolean changed = create;

            if (dto.contentType != null && !dto.contentType.equals(q.getContentType())) {
                q.setContentType(dto.contentType.length() > 0 ? dto.contentType : null);
                changed = true;
            }

            if (dto.maxSize != null && dto.maxSize != q.getMaxSize()
                    || dto.maxPayloadSize != null && dto.maxPayloadSize != q.getMaxPayloadSize()) {
                long maxSize = q.getMaxSize();
                int maxPayloadSize = q.getMaxPayloadSize();

                if (dto.maxSize != null) {
                    maxSize = dto.maxSize;
                    if (maxSize < 1000000L) {
                        call.setCode(422, "maxSize must be at least 1000000 bytes");
                        return;
                    }
                }

                if (dto.maxPayloadSize != null) {
                    maxPayloadSize = dto.maxPayloadSize;
                    if (maxPayloadSize != 0) {
                        if (maxPayloadSize > maxSize / 3) {
                            call.setCode(422, "maxPayloadSize may not exceed 1/3 of maxSize (" + maxSize / 3 + ") bytes");
                            return;
                        }
                        if (maxPayloadSize < 1000) {
                            call.setCode(422, "maxPayloadSize must be at least 1000 bytes");
                            return;
                        }
                    }
                } else if (maxPayloadSize != 0 && maxPayloadSize > maxSize / 3) {
                    maxPayloadSize = (int)(maxSize / 3);
                }

                q.setMaxSize(maxSize);
                q.setMaxPayloadSize(maxPayloadSize);
                changed = true;
            }

            if (dto.warnAfter != null) {
                try {
                    int secs = convertDuration(dto.warnAfter);
                    if (secs != q.getWarnAfter()) {
                        q.setWarnAfter(secs);
                        changed = true;
                    }
                } catch (IllegalArgumentException e) {
                    call.setCode(422, "Invalid warnAfter value, expected duration");
                    return;
                }
            }

            if (dto.errorAfter != null) {
                try {
                    int secs = convertDuration(dto.errorAfter);
                    if (secs != q.getErrorAfter()) {
                        q.setErrorAfter(secs);
                        changed = true;
                    }
                } catch (IllegalArgumentException e) {
                    call.setCode(422, "Invalid errorAfter value, expected duration");
                    return;
                }
            }

            if (create) {
                for (int attempt = 0; ; ) {
                    q.setId(generateQueueId());
                    if (repo.findQueue(q.getId()) == null) break;
                    if (++attempt == 20) throw new IOException("Got " + attempt + " dup id's attempting to create queue?");
                }
            }

            if (create) {
                db = db.deepCopy();      // make a copy before we modify it
                Map<String, String> queues = db.getQueues();
                if (queues == null) db.setQueues(queues = new HashMap<String, String>());
                queues.put(id, q.getId());
                repo.updateDatabase(db);
            }

            if (changed) repo.updateQueue(q);
        }
        call.setCode(create ? 201 : 200, createQueueDTO(call, id, q));
    }

    private int convertDuration(Object v) throws IllegalArgumentException {
        if (v instanceof Number) return ((Number)v).intValue();
        if (v instanceof String) return DurationParser.parse((String)v);
        throw new IllegalArgumentException("Expected duration");
    }

    private String generateQueueId() {
        return Integer.toString(Math.abs(RND.nextInt()), 36);
    }

    @Override
    protected void delete(Call call, String id) throws IOException {
        String qid = call.getDatabase().getQidForQueue(id);
        if (qid == null) {
            call.setCode(404);
            return;
        }
        repo.deleteQueue(qid);
    }

    @Override
    protected Controller getController(Call call, String id, String resource) throws IOException {
        String qid = call.getDatabase().getQidForQueue(id);
        Queue q = qid == null ? null : repo.findQueue(qid);
        if (q != null) {
            call.setQueue(q);
            if ("messages".equals(resource)) return messageController;
            if ("out".equals(resource)) return outputController;
            if ("timeline".equals(resource)) return timelineController;
        }
        return StatusCodeController.SC_404;
    }
}
