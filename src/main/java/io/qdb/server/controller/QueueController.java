package io.qdb.server.controller;

import io.qdb.kvstore.OptimisticLockingException;
import io.qdb.server.model.*;
import io.qdb.server.model.Queue;

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

    private static final SecureRandom RND = new SecureRandom();

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
                TimelineController timelineController) {
        super(jsonService);
        this.repo = repo;
        this.messageController = messageController;
        this.timelineController = timelineController;
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
    protected void create(Call call) throws IOException {
        QueueDTO dto = getBodyObject(call, QueueDTO.class);
        if (dto.id == null) {
            call.setCode(400, "id is required");
            return;
        }
        if (!VALID_QUEUE_ID.matcher(dto.id).matches()) {
            call.setCode(400, "id must contain only letters, numbers, hyphens and underscores");
            return;
        }

        Database db = call.getDatabase();
        Map<String, String> queues = db.getQueues();
        if (queues == null) db.setQueues(queues = new HashMap<String, String>());
        if (queues.containsKey(dto.id)) {
            call.setCode(400, "queue [" + dto.id + "] already exists");
            return;
        }

        if (dto.maxSize == null) {
            call.setCode(400, "maxSize is required");
            return;
        }

        Queue q = new Queue();
        q.setMaxPayloadSize(128 * 1024);
        q.setContentType("application/json; charset=utf-8");
        if (!updateAttributes(q, dto, call)) return;
        q.setDatabase(db.getId());

        for (int attempt = 0; ; ) {
            q.setId(generateQueueId());
            try {
                repo.createQueue(q);
                break;
            } catch (DuplicateIdException ignore) {
                if (++attempt == 20) throw new IOException("Got " + attempt + " dup id's attempting to create queue?");
            }
        }

        for (int attempt = 0; ; ) {
            queues.put(dto.id, q.getId());
            try {
                repo.updateDatabase(db);
                break;
            } catch (OptimisticLockingException e) {
                if (++attempt == 20) throw new IOException("Got " + attempt + " opt lock errors attempting to update db?");
                db = repo.findDatabase(db.getId());
                if (db == null) {
                    call.setCode(410);
                    return;
                }
                queues = db.getQueues();
            }
        }

        call.setCode(201, new QueueDTO(dto.id, q));
    }

    private String generateQueueId() {
        return Integer.toString(Math.abs(RND.nextInt()), 36);
    }

    @Override
    protected void update(Call call, String id) throws IOException {
        String qid = call.getDatabase().getQid(id);
        Queue q;
        if (qid == null || (q = repo.findQueue(qid)) == null) call.setCode(404);
        else update(q, id, getBodyObject(call, QueueDTO.class), call);
    }

    private void update(Queue q, String id, QueueDTO dto, Call call) throws IOException {
        if (dto.version != null && !dto.version.equals(q.getVersion())) {
            call.setCode(409, new QueueDTO(id, q));
            return;
        }

        if (!updateAttributes(q, dto, call)) return;

        boolean databaseChanged = dto.database != null && !dto.database.equals(call.getDatabase().getId());
        if (databaseChanged) {
            Database newdb = repo.findDatabase(dto.database);
            if (newdb == null || !newdb.isVisibleTo(call.getUser())) {
                call.setCode(400, "database [" + dto.database + "] does not exist or you do not have access to it");
                return;
            }
            call.setCode(400, "update queue database not supported");
            return;
        }

        try {
            call.setJson(new QueueDTO(id, repo.updateQueue(q)));
        } catch (OptimisticLockingException e) {
            q = repo.findQueue(q.getId());
            if (q == null) call.setCode(410);
            else call.setCode(409, new QueueDTO(id, q));
        }
    }

    private boolean updateAttributes(Queue q, QueueDTO dto, Call call) throws IOException {
        if (dto.contentType != null) q.setContentType(dto.contentType.length() > 0 ? dto.contentType : null);

        if (dto.maxSize != null || dto.maxPayloadSize != null) {
            long maxSize = q.getMaxSize();
            int maxPayloadSize = q.getMaxPayloadSize();

            if (dto.maxSize != null) {
                maxSize = dto.maxSize;
                if (maxSize < 1000000L) {
                    call.setCode(400, "maxSize must be at least 1000000 bytes");
                    return false;
                }
            }

            if (dto.maxPayloadSize != null) {
                maxPayloadSize = dto.maxPayloadSize;
                if (maxPayloadSize != 0) {
                    if (maxPayloadSize > maxSize / 3) {
                        call.setCode(400, "maxPayloadSize may not exceed 1/3 of maxSize (" + maxSize / 3 + ") bytes");
                        return false;
                    }
                    if (maxPayloadSize < 1000) {
                        call.setCode(400, "maxPayloadSize must be at least 1000 bytes");
                        return false;
                    }
                }
            } else if (maxPayloadSize != 0 && maxPayloadSize > maxSize / 3) {
                maxPayloadSize = (int)(maxSize / 3);
            }

            q.setMaxSize(maxSize);
            q.setMaxPayloadSize(maxPayloadSize);
        }

        return true;
    }

    @Override
    protected Controller getController(Call call, String id, String resource) throws IOException {
        String qid = call.getDatabase().getQid(id);
        Queue q = qid == null ? null : repo.findQueue(qid);
        if (q != null) {
            call.setQueue(q);
            if ("messages".equals(resource)) return messageController;
            if ("timeline".equals(resource)) return timelineController;
        }
        return StatusCodeController.SC_404;
    }
}
