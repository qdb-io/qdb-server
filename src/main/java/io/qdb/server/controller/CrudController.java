package io.qdb.server.controller;

import com.sun.tools.internal.ws.processor.modeler.ModelerException;
import io.qdb.server.JsonService;
import io.qdb.server.model.Database;
import io.qdb.server.model.ModelException;
import io.qdb.server.model.ModelObject;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.List;

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
                if (call.isGet()) list(call, call.getInt("offset", 0), call.getInt("limit", 30));
                else if (call.isPost()) create(call);
                else call.setCode(400);
            } else {
                String resource = call.nextSegment();
                if (resource == null) {
                    if (call.isGet()) show(call, id);
                    else if (call.isPut()) update(call, id);
                    else if (call.isDelete()) delete(call, id);
                    else call.setCode(400);
                } else {
                    getController(call, id, resource).handle(call);
                }
            }
        } catch (ModelException e) {
            call.setCode(400, e.getMessage());
        }
    }

    protected void list(Call call, int offset, int limit) throws IOException {
        call.setCode(404);
    }

    protected void show(Call call, String id) throws IOException {
        call.setCode(404);
    }

    protected void create(Call call) throws IOException {
        call.setCode(400);
    }

    protected void update(Call call, String id) throws IOException {
        call.setCode(400);
    }

    protected void delete(Call call, String id) throws IOException {
        call.setCode(400);
    }

    protected Controller getController(Call call, String id, String resource) throws IOException {
        return StatusCodeController.SC_404;
    }

    protected <T extends ModelObject> T getBodyObject(Call call, Class<T> cls) throws IOException {
        InputStream ins = Channels.newInputStream(call.getRequest().getByteChannel());
        try {
            return jsonService.fromJson(ins, cls);
        } finally {
            ins.close();
        }
    }

    public static class ListResult {

        public int offset;
        public int limit;
        public int total;
        public List data;

        public ListResult(int offset, int limit, List data) {
            this.offset = offset;
            this.limit = limit;
            this.data = data;
            int sz = data.size();
            total = sz < limit && (sz > 0 || offset == 0) ? offset + sz : -1;
        }
    }

}
