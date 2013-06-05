package io.qdb.server.controller;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import io.qdb.server.databind.DataBinder;
import io.qdb.server.databind.HasAnySetter;
import io.qdb.server.model.Output;
import io.qdb.server.model.Queue;
import io.qdb.server.output.OutputHandler;
import io.qdb.server.repo.Repository;
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

    public static class OutputDTO implements Comparable<OutputDTO>, HasAnySetter {

        public String id;
        public String oid;
        public Integer version;
        public String queue;
        public String type;
        public String url;
        public Boolean enabled;
        public Long messageId;
        public Long timestamp;
        public Integer updateIntervalMs;
        public transient Map<String, Object> params;

        @SuppressWarnings("UnusedDeclaration")
        public OutputDTO() { }

        public OutputDTO(String id, Output o) {
            this.id = id;
            this.oid = o.getId();
            this.version = o.getVersion();
            this.queue = o.getQueue();
            this.type = o.getType();
            this.url = o.getUrl();
            this.enabled = o.isEnabled();
            this.messageId = o.getMessageId();
            this.timestamp = o.getTimestamp();
            this.params = o.getParams();
        }

        @JsonAnySetter
        public void set(String key, Object value) {
            if (params == null) params = new HashMap<String, Object>();
            if (value == null) params.remove(key);
            else params.put(key, value);
        }

        @JsonAnyGetter
        public Map<String, Object> getParams() {
            return params;
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

            String oid = q.getOidForOutput(id);
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
                o.setUpdateIntervalMs(1000);
                o.setMessageId(-1);
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

            if (dto.url != null && !dto.url.equals(o.getUrl())) {
                o.setUrl(dto.url);
                changed = true;
            }

            if (dto.enabled != null && dto.enabled != o.isEnabled()) {
                o.setEnabled(dto.enabled);
                changed = true;
            }

            if (dto.updateIntervalMs != null && dto.updateIntervalMs != o.getUpdateIntervalMs()) {
                o.setUpdateIntervalMs(dto.updateIntervalMs);
                changed = true;
            }

            // user can set the timestamp (to start/restart processing from that time) or messageId but not both
            if (dto.timestamp != null && dto.timestamp != o.getTimestamp()) {
                o.setTimestamp(dto.timestamp);
                o.setMessageId(-2);
                changed = true;
            } else if (dto.messageId != null && dto.messageId != o.getMessageId()) {
                o.setTimestamp(0);
                o.setMessageId(dto.messageId);
                changed = true;
            }

            if (dto.params != null) {
                OutputHandler h = handlerFactory.createHandler(o.getType());
                new DataBinder().bind(dto.params, h).check();
                Map<String, Object> op = o.getParams();
                if (op == null) {
                    o.setParams(dto.params);
                    changed = true;
                } else {
                    for (Map.Entry<String, Object> e : dto.params.entrySet()) {
                        String key = e.getKey();
                        Object v = e.getValue();
                        Object existing = op.get(key);
                        if (!v.equals(existing)) {
                            op.put(key, v);
                            changed = true;
                        }
                    }
                }
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

    @Override
    protected void delete(Call call, String id) throws IOException {
        String oid = call.getQueue().getOidForOutput(id);
        if (oid == null) {
            call.setCode(404);
            return;
        }
        repo.deleteOutput(oid);
    }
}
