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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import io.qdb.buffer.MessageBuffer;
import io.qdb.server.Util;
import io.qdb.server.databind.DataBinder;
import io.qdb.server.databind.HasAnySetter;
import io.qdb.server.model.Output;
import io.qdb.server.model.Queue;
import io.qdb.server.output.OutputHandler;
import io.qdb.server.queue.QueueManager;
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
    private final QueueManager queueManager;

    private static final SecureRandom RND = new SecureRandom();

    private static final Logger log = LoggerFactory.getLogger(OutputController.class);

    public static class OutputDTO implements Comparable<OutputDTO>, HasAnySetter {

        public String id;
        public Integer version;
        public String type;
        public String url;
        public Boolean enabled;
        public Long fromId;
        public Long toId;
        public Long atId;
        public Date from;
        public Date to;
        public Date at;
        public Integer updateIntervalMs;
        public Object behindBy;
        public Long behindByBytes;
        public Double behindByPercentage;
        public transient Map<String, Object> params;

        @SuppressWarnings("UnusedDeclaration")
        public OutputDTO() { }

        public OutputDTO(String id, Output o, MessageBuffer mb, boolean borg) {
            this.id = id;
            this.version = o.getVersion();
            this.type = o.getType();
            this.url = o.getUrl();
            this.enabled = o.isEnabled();
            this.fromId = toLong(o.getFromId());
            this.toId = toLong(o.getToId());
            this.atId = toLong(o.getAtId());
            this.from = toDate(o.getFrom());
            this.to = toDate(o.getTo());
            this.at = toDate(o.getAt());
            this.params = o.getParams();
            if (at != null && mb != null) {
                try {
                    Date end = mb.getMostRecentTimestamp();
                    if (to != null && to.before(end)) end = to;
                    long ms = end.getTime() - at.getTime();
                    behindBy = borg ? ms : Util.humanDuration(ms);
                    behindByBytes = mb.getNextId() - atId;
                    behindByPercentage = Math.round(behindByBytes * 1000.0 / mb.getMaxSize()) / 10.0;
                } catch (IOException e) {
                    log.error(mb + ": " + e, e);
                }
            }
        }

        private Date toDate(long ms) {
            return ms == 0 ? null : new Date(ms);
        }

        private Long toLong(long id) {
            return id < 0 ? null : id;
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
    public OutputController(JsonService jsonService, Repository repo, OutputHandlerFactory handlerFactory,
                            QueueManager queueManager) {
        super(jsonService);
        this.repo = repo;
        this.handlerFactory = handlerFactory;
        this.queueManager = queueManager;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        List<OutputDTO> ans = new ArrayList<OutputDTO>();
        Queue q = call.getQueue();
        Map<String, String> outputs = q.getOutputs();
        if (outputs != null) {
            for (Map.Entry<String, String> e : outputs.entrySet()) {
                Output o = repo.findOutput(e.getValue());
                if (o != null) ans.add(createOutputDTO(call, e.getKey(), o, q));
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
        Queue q = call.getQueue();
        Map<String, String> outputs = q.getOutputs();
        if (outputs != null) {
            String oid = outputs.get(id);
            if (oid != null) {
                Output o = repo.findOutput(oid);
                if (o != null) {
                    call.setJson(createOutputDTO(call, id, o, q));
                    return;
                }
            }
        }
        call.setCode(404);
    }

    private OutputDTO createOutputDTO(Call call, String id, Output o, Queue q) throws IOException {
        return new OutputDTO(id, o, queueManager.getBuffer(q), call.getBoolean("borg"));
    }

    @Override
    protected void createOrUpdate(Call call, String id) throws IOException {
        OutputDTO dto = getBodyObject(call, OutputDTO.class);
        boolean create;
        Output o;
        Queue q;
        synchronized (repo) {
            // re-lookup queue inside sync block in case we need to update it
            q = repo.findQueue(call.getQueue().getId());
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
                o.setAtId(-1);
                o.setFromId(-1);
                o.setToId(-1);
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
                    call.setCode(409, createOutputDTO(call, id, o, q));
                    return;
                }
                o = o.deepCopy();
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

            // user can set the from (to start/restart processing from that time) or fromId but not both
            if (dto.from != null) {
                long ms = dto.from.getTime();
                if (ms != o.getFrom() || ms != o.getAt()) {
                    o.setFrom(ms);
                    o.setAt(ms);
                    o.setAtId(-2);
                    o.setFromId(-1);
                    changed = true;
                }
            } else if (dto.fromId != null) {
                Long fromId = dto.fromId;
                if (fromId != o.getFromId() || fromId != o.getAtId()) {
                    o.setFromId(fromId);
                    o.setAtId(fromId);
                    o.setFrom(0);
                    o.setTo(0);
                    o.setAt(0);
                    changed = true;
                }
            }

            // can set the to or toId (to stop processing at that time/id) but not both
            if (dto.to != null) {
                if (dto.to.getTime() != o.getTo()) {
                    o.setTo(dto.to.getTime());
                    o.setToId(-1);
                    changed = true;
                }
            } else if (dto.toId != null && dto.toId != o.getToId()) {
                o.setToId(dto.toId);
                o.setTo(0);
                changed = true;
            }

            if (dto.params != null) {
                OutputHandler h = handlerFactory.createHandler(o.getType());
                new DataBinder(jsonService).updateMap(true).bind(dto.params, h).check();
                Map<String, Object> params = o.getParams();
                if (params == null) {
                    o.setParams(dto.params);
                    changed = true;
                } else {
                    for (Map.Entry<String, Object> e : dto.params.entrySet()) {
                        String key = e.getKey();
                        Object v = e.getValue();
                        Object existing = params.get(key);
                        if (!v.equals(existing)) {
                            params.put(key, v);
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

            if (create) {
                q = q.deepCopy();      // make a copy before we modify it
                Map<String, String> outputs = q.getOutputs();
                if (outputs == null) q.setOutputs(outputs = new HashMap<String, String>());
                outputs.put(id, o.getId());
                repo.updateQueue(q);
            }

            if (changed) repo.updateOutput(o);
        }
        call.setCode(create ? 201 : 200, createOutputDTO(call, id, o, q));
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
