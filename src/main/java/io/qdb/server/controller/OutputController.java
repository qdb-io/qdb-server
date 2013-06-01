package io.qdb.server.controller;

import io.qdb.server.model.Output;
import io.qdb.server.model.Queue;
import io.qdb.server.model.Repository;
import io.qdb.server.output.OutputHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;
import java.util.regex.Pattern;

@Singleton
public class OutputController extends CrudController {

    private final Repository repo;
    private final OutputHandlerFactory handlerFactory;

    private static final SecureRandom RND = new SecureRandom();

    private static final Logger log = LoggerFactory.getLogger(OutputController.class);

    public static class OutputDTO implements Comparable<OutputDTO> {

        public String id;
        public String oid;
        public Integer version;
        public String queue;
        public String type;
        public boolean enabled;
        public long messageId;

        @SuppressWarnings("UnusedDeclaration")
        public OutputDTO() { }

        public OutputDTO(String id, Output o) {
            this.id = id;
            this.oid = o.getId();
            this.version = o.getVersion();
            this.queue = o.getQueue();
            this.type = o.getType();
            this.enabled = o.isEnabled();
            this.messageId = o.getMessageId();
        }

        @Override
        public int compareTo(OutputDTO o) {
            return id.compareTo(o.id);
        }
    }

    private static final Pattern VALID_OUTPUT_ID = Pattern.compile("[0-9a-z\\-_]+", Pattern.CASE_INSENSITIVE);

    @Inject
    public OutputController(JsonService jsonService, Repository repo, OutputHandlerFactory handlerFactory) {
        super(jsonService);
        this.repo = repo;
        this.handlerFactory = handlerFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        List<OutputDTO> ans = new ArrayList<OutputDTO>();
        Map<String, String> outputs = call.getQueue().getOutputs();
        if (outputs != null) {
            for (Map.Entry<String, String> e : outputs.entrySet()) {
                Output o = repo.findOutput(e.getValue());
                if (o != null) ans.add(new OutputDTO(e.getKey(), o));
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
        Map<String, String> outputs = call.getQueue().getOutputs();
        call.setJson(new Count(outputs == null ? 0 : outputs.size()));
    }

    @Override
    protected void show(Call call, String id) throws IOException {
        Map<String, String> outputs = call.getQueue().getOutputs();
        if (outputs != null) {
            String oid = outputs.get(id);
            if (oid != null) {
                Output o = repo.findOutput(oid);
                if (o != null) {
                    call.setJson(new OutputDTO(id, o));
                    return;
                }
            }
        }
        call.setCode(404);
    }

    @Override
    protected void createOrUpdate(Call call, String id) throws IOException {
        OutputDTO dto = getBodyObject(call, OutputDTO.class);
        boolean create;
        Output o;
        synchronized (repo) {
            // re-lookup queue inside sync block in case we need to update it
            Queue q = repo.findQueue(call.getQueue().getId());
            if (q == null) {   // this isn't likely but isn't impossible either
                call.setCode(404);
                return;
            }

            String oid = q.getOid(id);
            if (create = oid == null) {
                if (call.isPut()) {
                    call.setCode(404);
                    return;
                }
                if (!VALID_OUTPUT_ID.matcher(id).matches()) {
                    call.setCode(400, "Output id must contain only letters, numbers, hyphens and underscores");
                    return;
                }
                if (dto.type == null) {
                    call.setCode(400, "type is required");
                    return;
                }
                o = new Output();
                o.setQueue(q.getId());
                o.setEnabled(true);
            } else {
                o = repo.findOutput(oid);
                if (o == null) {    // this shouldn't happen
                    String msg = "Output /databases/" + q.getDatabase() + "/queues/" + q.getId() + "/outputs/" + id +
                            " oid [" + oid + "] not found";
                    log.error(msg);
                    call.setCode(500, msg);
                    return;
                }
                if (dto.version != null && !dto.version.equals(o.getVersion())) {
                    call.setCode(409, new OutputDTO(id, o));
                    return;
                }
                o = (Output)o.clone();
            }

            boolean changed = create;

            if (dto.type != null && !dto.type.equals(o.getType())) {
                try {
                    handlerFactory.createHandler(dto.type);
                } catch (IllegalArgumentException e) {
                    call.setCode(400, e.getMessage());
                    return;
                }
                o.setType(dto.type);
                changed = true;
            }

            if (create) {
                for (int attempt = 0; ; ) {
                    o.setId(generateOutputId());
                    if (repo.findOutput(o.getId()) == null) break;
                    if (++attempt == 20) throw new IOException("Got " + attempt + " dup id's attempting to create output?");
                }
            }

            if (changed) repo.updateOutput(o);

            if (create) {
                // update the queue after the output to avoid having a queue referencing an output that does not exist if
                // something goes wrong - better to have an unreferenced output lying around
                q = (Queue)q.clone();      // make a copy before we modify it
                Map<String, String> outputs = q.getOutputs();
                if (outputs == null) q.setOutputs(outputs = new HashMap<String, String>());
                outputs.put(id, o.getId());
                repo.updateQueue(q);
            }
        }
        call.setCode(create ? 201 : 200, new OutputDTO(id, o));
    }

    private String generateOutputId() {
        return Integer.toString(Math.abs(RND.nextInt()), 36);
    }
}
