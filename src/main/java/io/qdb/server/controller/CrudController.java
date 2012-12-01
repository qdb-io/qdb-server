package io.qdb.server.controller;

import java.io.IOException;

/**
 * Base class for controllers that provide CRUD for some resource.
 */
public abstract class CrudController {

    protected CrudController() {
    }

    public void handle(Call call) throws IOException {
        String id = call.nextSegment();
        if (id == null) {
            if (call.isGet()) list(call);
            else if (call.isPost()) create(call);
            else call.setCode(400);
        } else {
            String seg = call.nextSegment();
            if (seg == null) {
                if (call.isGet()) show(call, id);
                else if (call.isPut()) update(call, id);
                else if (call.isDelete()) delete(call, id);
                else call.setCode(400);
                // todo handle sub-resources
            }
        }
    }

    protected void list(Call call) throws IOException {
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

}
