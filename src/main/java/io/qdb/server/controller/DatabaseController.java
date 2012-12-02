package io.qdb.server.controller;

import io.qdb.server.model.Database;
import io.qdb.server.model.Repository;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

@Singleton
public class DatabaseController extends CrudController {

    private final Repository repo;

    @Inject
    public DatabaseController(Repository repo) {
        this.repo = repo;
    }

    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        call.setJson(repo.findDatabasesVisibleTo(call.getUser()));
    }

    @Override
    protected void show(Call call, String id) throws IOException {
        Database db = repo.findDatabase(id);
        if (db == null) {
            call.setCode(404);
        } else if (db.isVisibleTo(call.getUser())) {
            call.setJson(db);
        } else {
            call.setCode(403);
        }
    }

    @Override
    protected Controller getController(Call call, String id, String resource) throws IOException {
        Database db = repo.findDatabase(id);
        if (db != null) {
            if (!db.isVisibleTo(call.getUser())) return StatusCodeController.SC_403;
            if ("queues".equals(resource)) return new QueueController(repo, db);
        }
        return StatusCodeController.SC_404;
    }
}
