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
import io.qdb.server.databind.DataBinder;
import io.qdb.server.databind.DurationParser;
import io.qdb.server.databind.HasAnySetter;
import io.qdb.server.filter.GrepMessageFilter;
import io.qdb.server.filter.MessageFilterFactory;
import io.qdb.server.filter.RoutingKeyMessageFilter;
import io.qdb.server.model.Output;
import io.qdb.server.model.Queue;
import io.qdb.server.monitor.Status;
import io.qdb.server.output.OutputHandler;
import io.qdb.server.output.OutputStatusMonitor;
import io.qdb.server.queue.QueueManager;
import io.qdb.server.repo.Repository;
import io.qdb.server.output.OutputHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

@Singleton
public class OutputController extends CrudController {

    private final Repository repo;
    private final OutputHandlerFactory handlerFactory;
    private final QueueManager queueManager;
    private final OutputStatusMonitor outputStatusMonitor;
    private final MessageFilterFactory messageFilterFactory;

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
        public Long limit;
        public Integer updateIntervalMs;
        public String status;
        public Object behindBy;
        public Long behindByBytes;
        public Double behindByPercentage;
        public Double warnAfter;
        public Double errorAfter;
        public String filter;
        public String routingKey;
        public String grep;
        public transient Map<String, Object> params;

        @SuppressWarnings("UnusedDeclaration")
        public OutputDTO() { }

        public OutputDTO(String id, Output o) {
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
            long limit = o.getLimit();
            this.limit = limit <= 0 ? null : limit;
            this.updateIntervalMs = o.getUpdateIntervalMs();
            this.warnAfter = toPercentage(o.getWarnAfter());
            this.errorAfter = toPercentage(o.getErrorAfter());
            this.filter = o.getFilter();
            this.routingKey = o.getRoutingKey();
            this.grep = o.getGrep();
            this.params = o.getParams();
        }

        private Date toDate(long ms) {
            return ms == 0 ? null : new Date(ms);
        }

        private Long toLong(long v) {
            return v < 0 ? null : v;
        }

        private Double toPercentage(double p) {
            return p <= 0.0 ? null : p;
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
                            QueueManager queueManager, OutputStatusMonitor outputStatusMonitor,
                            MessageFilterFactory messageFilterFactory) {
        super(jsonService);
        this.repo = repo;
        this.handlerFactory = handlerFactory;
        this.queueManager = queueManager;
        this.outputStatusMonitor = outputStatusMonitor;
        this.messageFilterFactory = messageFilterFactory;
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
        OutputDTO dto = new OutputDTO(id, o);
        MessageBuffer mb = queueManager.getBuffer(q);
        if (mb != null) {
            boolean borg = call.getBoolean("borg");
            try {
                Date end = mb.getMostRecentTimestamp();
                if (end != null) {
                    if (dto.to != null && dto.to.before(end)) end = dto.to;
                    if (dto.at != null) {
                        long ms = end.getTime() - dto.at.getTime();
                        dto.behindBy = borg ? ms : DurationParser.formatHumanMs(ms);
                    }
                    dto.behindByBytes = mb.getNextId() - (dto.atId == null ? dto.fromId == null ? mb.getOldestId() : dto.fromId : dto.atId);
                    if (dto.behindByBytes < 0) dto.behindByBytes = 0L;
                } else {
                    dto.behindByBytes = 0L;
                }
                dto.behindByPercentage = Math.round(dto.behindByBytes * 1000.0 / mb.getMaxSize()) / 10.0;

                Status status = outputStatusMonitor.getStatus(o);
                if (status != null) dto.status = status.toString();
            } catch (IOException e) {
                log.error(mb + ": " + e, e);
            }
        }
        return dto;
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
                    call.setCode(422, "Output id must contain only letters, numbers, hyphens and underscores");
                    return;
                }
                if (dto.type == null) {
                    call.setCode(422, "type is required");
                    return;
                }
                MessageBuffer mb = queueManager.getBuffer(q);
                if (mb == null) {
                    call.setCode(503, "queue buffer is unavailable");
                    return;
                }
                o = new Output();
                o.setQueue(q.getId());
                o.setEnabled(true);
                o.setUpdateIntervalMs(1000);
                o.setAtId(mb.getNextId());
                o.setFromId(-1);
                o.setToId(-1);
            } else {
                o = repo.findOutput(oid);
                if (o == null) {    // this shouldn't happen
                    String msg = "Output /db/" + q.getDatabase() + "/q/" + q.getId() + "/out/" + id +
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
                    call.setCode(422, e.getMessage());
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
                    o.setAtId(-1);
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

            if (dto.limit != null && dto.limit != o.getLimit()) {
                o.setLimit(dto.limit);
                changed = true;
            }

            if (dto.warnAfter != null && Math.abs(dto.warnAfter - o.getWarnAfter()) >= 0.001) {
                o.setWarnAfter(dto.warnAfter);
                changed = true;
            }

            if (dto.errorAfter != null && Math.abs(dto.errorAfter - o.getErrorAfter()) >= 0.001) {
                o.setErrorAfter(dto.errorAfter);
                changed = true;
            }

            if (dto.filter != null && !dto.filter.equals(o.getFilter())) {
                if (dto.filter.length() > 0) {
                    try {
                        messageFilterFactory.createFilter(dto.filter);
                    } catch (IllegalArgumentException e) {
                        call.setCode(422, e.getMessage());
                        return;
                    }
                }
                o.setFilter(dto.filter);
                changed = true;
            }

            if (dto.routingKey != null && !dto.routingKey.equals(o.getRoutingKey())) {
                if (dto.routingKey.length() > 0) {
                    RoutingKeyMessageFilter mf = new RoutingKeyMessageFilter();
                    mf.routingKey = dto.routingKey;
                    try {
                        mf.init(null);
                    } catch (IllegalArgumentException e) {
                        call.setCode(422, e.getMessage());
                        return;
                    }
                }
                o.setRoutingKey(dto.routingKey);
                changed = true;
            }

            if (dto.grep != null && !dto.grep.equals(o.getGrep())) {
                if (dto.grep.length() > 0) {
                    GrepMessageFilter mf = new GrepMessageFilter();
                    mf.grep = dto.grep;
                    try {
                        mf.init(null);
                    } catch (IllegalArgumentException e) {
                        call.setCode(422, e.getMessage());
                        return;
                    }
                }
                o.setGrep(dto.grep);
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
                    o.setId(generateId());
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
