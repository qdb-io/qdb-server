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

import io.qdb.kvstore.KeyValueStoreException;
import io.qdb.server.databind.DataBinder;
import io.qdb.server.databind.DataBindingException;
import io.qdb.server.databind.DurationParser;
import org.simpleframework.http.Request;

import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;

/**
 * Base class for controllers that provide CRUD for some resource.
 */
public abstract class CrudController implements Controller {

    protected final JsonService jsonService;

    private static final SecureRandom RND = new SecureRandom();

    protected CrudController(JsonService jsonService) {
        this.jsonService = jsonService;
    }

    public void handle(Call call) throws IOException {
        try {
            String id = call.nextSegment();
            if (id == null) {
                if (call.isGet()) {
                    if (call.getBoolean("count")) {
                        count(call);
                    } else {
                        int offset = call.getInt("offset", 0);
                        list(call, offset, call.getInt("limit", Integer.MAX_VALUE - offset));
                    }
                } else if (call.isPost()) {
                    create(call);
                } else {
                    call.setCode(400);
                }
            } else {
                String resource = call.nextSegment();
                if (resource == null) {
                    if (call.isGet()) show(call, id);
                    else if (call.isPost() || call.isPut()) createOrUpdate(call, id);
                    else if (call.isDelete()) delete(call, id);
                    else call.setCode(400);
                } else {
                    getController(call, id, resource).handle(call);
                }
            }
        } catch (DataBindingException e) {
            call.setCode(422, e.getErrors());
        } catch (BadRequestException e) {
            call.setCode(e.getStatus(), e.getMessage());
        } catch (KeyValueStoreException e) {
            call.setCode(400, e.getMessage());
        }
    }

    protected void list(Call call, int offset, int limit) throws IOException {
        call.setCode(404);
    }

    protected void count(Call call) throws IOException {
        call.setCode(400, "Count not supported");
    }

    protected void show(Call call, String id) throws IOException {
        call.setCode(404);
    }

    protected void create(Call call) throws IOException {
        call.setCode(400, "Create not supported");
    }

    protected void createOrUpdate(Call call, String id) throws IOException {
        call.setCode(400, "Create not supported");
    }

    protected void update(Call call, String id) throws IOException {
        call.setCode(400, "Update not supported");
    }

    protected void delete(Call call, String id) throws IOException {
        call.setCode(400, "Delete not supported");
    }

    protected Controller getController(Call call, String id, String resource) throws IOException {
        return StatusCodeController.SC_404;
    }

    /**
     * Create an instance of cls from the body (if it is JSON) or the form parameters otherwise.
     * Throws BadRequestException or DataBindingException for invalid fields or JSON.
     */
    protected <T> T getBodyObject(Call call, Class<T> cls) throws IOException {
        Request req = call.getRequest();
        T dto;
        if ("application/json".equals(req.getValue("Content-Type"))) {
            InputStream ins = req.getInputStream();
            try {
                dto = jsonService.fromJson(ins, cls);
            } catch (IllegalArgumentException x) {
                throw new BadRequestException(400, x.getMessage());
            } finally {
                ins.close();
            }
        } else { // assume form data and populate instance with params that match field names of the dto
            try {
                dto = cls.newInstance();
            } catch (Exception x) {
                throw new RuntimeException(x.toString(), x);
            }
            new DataBinder(jsonService).bind(req.getForm(), dto).check();
        }
        return dto;
    }

    protected int convertDuration(Object v) throws IllegalArgumentException {
        if (v instanceof Number) return ((Number)v).intValue();
        if (v instanceof String) return DurationParser.parse((String) v);
        throw new IllegalArgumentException("Expected duration");
    }

    protected String generateId() {
        return Integer.toString(Math.abs(RND.nextInt()), 36);
    }

    public static class Count {

        public int count;

        public Count(int count) {
            this.count = count;
        }
    }
}
