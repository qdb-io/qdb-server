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
import io.qdb.server.databind.DataBinder;
import io.qdb.server.databind.DurationParser;
import io.qdb.server.databind.HasAnySetter;
import io.qdb.server.input.InputHandler;
import io.qdb.server.input.InputHandlerFactory;
import io.qdb.server.input.InputStatusMonitor;
import io.qdb.server.model.Input;
import io.qdb.server.model.Queue;
import io.qdb.server.monitor.Status;
import io.qdb.server.repo.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

@Singleton
public class InputController extends CrudController {

    private final Repository repo;
    private final InputHandlerFactory handlerFactory;
    private final InputStatusMonitor inputStatusMonitor;

    private static final Logger log = LoggerFactory.getLogger(InputController.class);

    public static class InputDTO implements Comparable<InputDTO>, HasAnySetter {

        public String id;
        public Integer version;
        public String type;
        public String url;
        public Boolean enabled;
        public Integer updateIntervalMs;
        public Object warnAfter;
        public Object errorAfter;
        public transient Map<String, Object> params;

        public String status;
        public Long lastMessageId;
        public Date lastMessageTimestamp;
        public Object lastMessageAppended;

        @SuppressWarnings("UnusedDeclaration")
        public InputDTO() { }

        public InputDTO(String id, Input in, boolean borg) {
            this.id = id;
            version = in.getVersion();
            type = in.getType();
            url = in.getUrl();
            enabled = in.isEnabled();
            updateIntervalMs = in.getUpdateIntervalMs();
            params = in.getParams();
            lastMessageId = null0(in.getLastMessageId());
            lastMessageTimestamp = toDate(in.getLastMessageTimestamp());

            if (borg) {
                this.warnAfter = null0(in.getWarnAfter());
                this.errorAfter = null0(in.getErrorAfter());
            } else {
                int secs = in.getWarnAfter();
                if (secs > 0) this.warnAfter = DurationParser.formatHumanMs(secs * 1000L);
                secs = in.getErrorAfter();
                if (secs > 0) this.errorAfter = DurationParser.formatHumanMs(secs * 1000L);
            }
        }

        private Date toDate(long ms) {
            return ms == 0 ? null : new Date(ms);
        }

        private Integer null0(int x) {
            return x == 0 ? null : x;
        }

        private Long null0(long x) {
            return x == 0 ? null : x;
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
        public int compareTo(InputDTO in) {
            return id.compareTo(in.id);
        }
    }

    private static final Pattern VALID_INPUT_ID = Pattern.compile("[0-9a-z\\-_]+", Pattern.CASE_INSENSITIVE);

    @Inject
    public InputController(JsonService jsonService, Repository repo, InputHandlerFactory handlerFactory,
                           InputStatusMonitor inputStatusMonitor) {
        super(jsonService);
        this.repo = repo;
        this.handlerFactory = handlerFactory;
        this.inputStatusMonitor = inputStatusMonitor;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        List<InputDTO> ans = new ArrayList<InputDTO>();
        Queue q = call.getQueue();
        Map<String, String> inputs = q.getInputs();
        if (inputs != null) {
            for (Map.Entry<String, String> e : inputs.entrySet()) {
                Input in = repo.findInput(e.getValue());
                if (in != null) ans.add(createInputDTO(call, e.getKey(), in));
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
        Map<String, String> inputs = call.getQueue().getInputs();
        call.setJson(new Count(inputs == null ? 0 : inputs.size()));
    }

    @Override
    protected void show(Call call, String id) throws IOException {
        Queue q = call.getQueue();
        Map<String, String> inputs = q.getInputs();
        if (inputs != null) {
            String inputId = inputs.get(id);
            if (inputId != null) {
                Input in = repo.findInput(inputId);
                if (in != null) {
                    call.setJson(createInputDTO(call, id, in));
                    return;
                }
            }
        }
        call.setCode(404);
    }

    private InputDTO createInputDTO(Call call, String id, Input in) throws IOException {
        boolean borg = call.getBoolean("borg");
        InputDTO dto = new InputDTO(id, in, borg);
        if (dto.lastMessageTimestamp != null) {
            long ms = System.currentTimeMillis() - dto.lastMessageTimestamp.getTime();
            dto.lastMessageAppended = borg ? ms : DurationParser.formatHumanMs(ms) + " ago";
        }
        Status status = inputStatusMonitor.getStatus(in);
        if (status != null) dto.status = status.toString();
        return dto;
    }

    @Override
    protected void createOrUpdate(Call call, String id) throws IOException {
        InputDTO dto = getBodyObject(call, InputDTO.class);
        boolean create;
        Input in;
        Queue q;
        synchronized (repo) {
            // re-lookup queue inside sync block in case we need to update it
            q = repo.findQueue(call.getQueue().getId());
            if (q == null) {   // this isn't likely but isn't impossible either
                call.setCode(404);
                return;
            }

            String inputId = q.getInputIdForInput(id);
            if (create = inputId == null) {
                if (call.isPut()) {
                    call.setCode(404);
                    return;
                }
                if (!VALID_INPUT_ID.matcher(id).matches()) {
                    call.setCode(400, "Input id must contain only letters, numbers, hyphens and underscores");
                    return;
                }
                if (dto.type == null) {
                    call.setCode(400, "type is required");
                    return;
                }
                in = new Input();
                in.setQueue(q.getId());
                in.setEnabled(true);
                in.setUpdateIntervalMs(1000);
            } else {
                in = repo.findInput(inputId);
                if (in == null) {    // this shouldn't happen
                    String msg = "Input /db/" + q.getDatabase() + "/q/" + q.getId() + "/in/" + id +
                            " inputId [" + inputId + "] not found";
                    log.error(msg);
                    call.setCode(500, msg);
                    return;
                }
                if (dto.version != null && !dto.version.equals(in.getVersion())) {
                    call.setCode(409, createInputDTO(call, id, in));
                    return;
                }
                in = in.deepCopy();
            }

            boolean changed = create;

            if (dto.type != null && !dto.type.equals(in.getType())) {
                try {
                    handlerFactory.createHandler(dto.type);
                } catch (IllegalArgumentException e) {
                    call.setCode(400, e.getMessage());
                    return;
                }
                in.setType(dto.type);
                changed = true;
            }

            if (dto.url != null && !dto.url.equals(in.getUrl())) {
                in.setUrl(dto.url);
                changed = true;
            }

            if (dto.enabled != null && dto.enabled != in.isEnabled()) {
                in.setEnabled(dto.enabled);
                changed = true;
            }

            if (dto.updateIntervalMs != null && dto.updateIntervalMs != in.getUpdateIntervalMs()) {
                in.setUpdateIntervalMs(dto.updateIntervalMs);
                changed = true;
            }

            if (dto.warnAfter != null) {
                try {
                    int secs = convertDuration(dto.warnAfter);
                    if (secs != in.getWarnAfter()) {
                        in.setWarnAfter(secs);
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
                    if (secs != in.getErrorAfter()) {
                        in.setErrorAfter(secs);
                        changed = true;
                    }
                } catch (IllegalArgumentException e) {
                    call.setCode(422, "Invalid errorAfter value, expected duration");
                    return;
                }
            }

            if (dto.params != null) {
                InputHandler h = handlerFactory.createHandler(in.getType());
                new DataBinder(jsonService).updateMap(true).bind(dto.params, h).check();
                Map<String, Object> params = in.getParams();
                if (params == null) {
                    in.setParams(dto.params);
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
                    in.setId(generateId());
                    if (repo.findInput(in.getId()) == null) break;
                    if (++attempt == 20) throw new IOException("Got " + attempt + " dup id's attempting to create input?");
                }
            }

            if (create) {
                q = q.deepCopy();      // make a copy before we modify it
                Map<String, String> inputs = q.getInputs();
                if (inputs == null) q.setInputs(inputs = new HashMap<String, String>());
                inputs.put(id, in.getId());
                repo.updateQueue(q);
            }

            if (changed) repo.updateInput(in);
        }
        call.setCode(create ? 201 : 200, createInputDTO(call, id, in));
    }

    @Override
    protected void delete(Call call, String id) throws IOException {
        String inputId = call.getQueue().getInputIdForInput(id);
        if (inputId == null) {
            call.setCode(404);
            return;
        }
        repo.deleteInput(inputId);
    }
}
