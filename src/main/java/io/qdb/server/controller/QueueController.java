package io.qdb.server.controller;

import io.qdb.server.JsonService;
import io.qdb.server.model.*;
import io.qdb.server.model.Queue;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;
import java.util.regex.Pattern;

public class QueueController extends CrudController {

    private final Repository repo;
    private Database db;

    private static final SecureRandom RND = new SecureRandom();

    public static class QueueDTO implements Comparable<QueueDTO> {

        public String id;
        public String qid;
        public Integer version;

        public QueueDTO() { }

        public QueueDTO(String id, Queue queue) {
            this.id = id;
            this.qid = queue.getId();
            this.version = queue.getVersion();
        }

        @Override
        public int compareTo(QueueDTO o) {
            return id.compareTo(o.id);
        }
    }

    private static final Pattern VALID_QUEUE_ID = Pattern.compile("[0-9a-z\\-_]+", Pattern.CASE_INSENSITIVE);

    public QueueController(JsonService jsonService, Repository repo, Database db) {
        super(jsonService);
        this.repo = repo;
        this.db = db;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        List<QueueDTO> ans = new ArrayList<QueueDTO>();
        Map<String, String> queues = db.getQueues();
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
        Map<String, String> queues = db.getQueues();
        call.setJson(new Count(queues == null ? 0 : queues.size()));
    }

    @Override
    protected void show(Call call, String id) throws IOException {
        Map<String, String> queues = db.getQueues();
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

        Map<String, String> queues = db.getQueues();
        if (queues == null) db.setQueues(queues = new HashMap<String, String>());
        if (queues.containsKey(dto.id)) {
            call.setCode(400, "queue [" + dto.id + "] already exists");
            return;
        }

        Queue queue = new Queue();
        queue.setDatabase(db.getId());
        for (int attempt = 0; ; ) {
            queue.setId(generateQueueId());
            try {
                repo.createQueue(queue);
                break;
            } catch (DuplicateIdException ignore) {
                if (++attempt == 20) throw new IOException("Got " + attempt + " dup id's attempting to create queue?");
            }
        }

        for (int attempt = 0; ; ) {
            queues.put(dto.id, queue.getId());
            try {
                repo.updateDatabase(db);
                break;
            } catch (OptLockException e) {
                if (++attempt == 20) throw new IOException("Got " + attempt + " opt lock errors attempting to update db?");
                db = repo.findDatabase(db.getId());
                if (db == null) {
                    call.setCode(410);
                    return;
                }
                queues = db.getQueues();
            }
        }

        call.setCode(201, new QueueDTO(dto.id, queue));
    }

    private String generateQueueId() {
        return Integer.toString(Math.abs(RND.nextInt()), 36);
    }
}
