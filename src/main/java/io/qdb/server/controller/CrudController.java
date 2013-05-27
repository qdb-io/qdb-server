package io.qdb.server.controller;

import io.qdb.kvstore.KeyValueStoreException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

/**
 * Base class for controllers that provide CRUD for some resource.
 */
public abstract class CrudController implements Controller {

    protected final JsonService jsonService;

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

    protected <T> T getBodyObject(Call call, Class<T> cls) throws IOException {
        InputStream ins = Channels.newInputStream(call.getRequest().getByteChannel());
        try {
            return jsonService.fromJson(ins, cls);
        } finally {
            ins.close();
        }
    }

    public static class Count {

        public int count;

        public Count(int count) {
            this.count = count;
        }
    }
}
