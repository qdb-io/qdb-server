package io.qdb.server.controller;

import java.io.IOException;

/**
 * Base class for controllers that provide CRUD for some resource.
 */
public abstract class CrudController implements Controller {

    protected CrudController() {
    }

    public void handle(Call call) throws IOException {
        String id = call.nextSegment();
        if (id == null) {
            if (call.isGet()) list(call);
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

    protected Controller getController(Call call, String id, String resource) throws IOException {
        return StatusCodeController.SC_404;
    }

}
