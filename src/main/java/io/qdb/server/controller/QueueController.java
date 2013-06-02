package io.qdb.server.controller;

import io.qdb.server.model.*;
import io.qdb.server.model.Queue;
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

    private static final SecureRandom RND = new SecureRandom();

    private static final Logger log = LoggerFactory.getLogger(QueueController.class);

    public static class QueueDTO implements Comparable<QueueDTO> {

        public String id;
        public String qid;
        public Integer version;
        public String database;
        public Long maxSize;
        public Integer maxPayloadSize;
        public String contentType;

        @SuppressWarnings("UnusedDeclaration")
        public QueueDTO() { }

        public QueueDTO(String id, Queue queue) {
            this.id = id;
            this.qid = queue.getId();
            this.version = queue.getVersion();
            this.maxSize = queue.getMaxSize();
            this.maxPayloadSize = queue.getMaxPayloadSize();
            this.contentType = queue.getContentType();
        }

        @Override
        public int compareTo(QueueDTO o) {
            return id.compareTo(o.id);
        }
    }

    private static final Pattern VALID_QUEUE_ID = Pattern.compile("[0-9a-z\\-_]+", Pattern.CASE_INSENSITIVE);

    @Inject
    public QueueController(JsonService jsonService, Repository repo, MessageController messageController,
                TimelineController timelineController, OutputController outputController) {
        super(jsonService);
        this.repo = repo;
        this.messageController = messageController;
        this.timelineController = timelineController;
        this.outputController = outputController;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        List<QueueDTO> ans = new ArrayList<QueueDTO>();
        Map<String, String> queues = call.getDatabase().getQueues();
        if (queues != null) {
            for (Map.Entry<String, String> e : queues.entrySet()) {
                Queue queue = repo.findQueue(e.getValue());
                if (queue != null) ans.add(new QueueDTO(e.getKey(), queue));
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
                    call.setJson(new QueueDTO(id, queue));
                    return;
                }
            }
        }
        call.setCode(404);
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
                    call.setCode(400, "Queue id must contain only letters, numbers, hyphens and underscores");
                    return;
                }
                if (dto.maxSize == null) {
                    call.setCode(400, "maxSize is required");
                    return;
                }
                q = new Queue();
                q.setDatabase(db.getId());
                q.setMaxPayloadSize(128 * 1024);
                q.setContentType("application/json; charset=utf-8");
            } else {
                q = repo.findQueue(qid);
                if (q == null) {    // this shouldn't happen
                    String msg = "Queue /databases/" + db.getId() + "/queues/" + id + " qid [" + qid +
                            "] not found";
                    log.error(msg);
                    call.setCode(500, msg);
                    return;
                }
                if (dto.version != null && !dto.version.equals(q.getVersion())) {
                    call.setCode(409, new QueueDTO(id, q));
                    return;
                }
                q = (Queue)q.clone();
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
                        call.setCode(400, "maxSize must be at least 1000000 bytes");
                        return;
                    }
                }

                if (dto.maxPayloadSize != null) {
                    maxPayloadSize = dto.maxPayloadSize;
                    if (maxPayloadSize != 0) {
                        if (maxPayloadSize > maxSize / 3) {
                            call.setCode(400, "maxPayloadSize may not exceed 1/3 of maxSize (" + maxSize / 3 + ") bytes");
                            return;
                        }
                        if (maxPayloadSize < 1000) {
                            call.setCode(400, "maxPayloadSize must be at least 1000 bytes");
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

            if (create) {
                for (int attempt = 0; ; ) {
                    q.setId(generateQueueId());
                    if (repo.findQueue(q.getId()) == null) break;
                    if (++attempt == 20) throw new IOException("Got " + attempt + " dup id's attempting to create queue?");
                }
            }

            if (changed) repo.updateQueue(q);

            if (create) {
                // update the db after the queue to avoid having a db referencing a queue that does not exist if
                // something goes wrong - better to have an unreferenced queue lying around
                db = (Database)db.clone();      // make a copy before we modify it
                Map<String, String> queues = db.getQueues();
                if (queues == null) db.setQueues(queues = new HashMap<String, String>());
                queues.put(id, q.getId());
                repo.updateDatabase(db);
            }
        }
        call.setCode(create ? 201 : 200, new QueueDTO(id, q));
    }

    private String generateQueueId() {
        return Integer.toString(Math.abs(RND.nextInt()), 36);
    }

    @Override
    protected Controller getController(Call call, String id, String resource) throws IOException {
        String qid = call.getDatabase().getQidForQueue(id);
        Queue q = qid == null ? null : repo.findQueue(qid);
        if (q != null) {
            call.setQueue(q);
            if ("messages".equals(resource)) return messageController;
            if ("outputs".equals(resource)) return outputController;
            if ("timeline".equals(resource)) return timelineController;
        }
        return StatusCodeController.SC_404;
    }
}
