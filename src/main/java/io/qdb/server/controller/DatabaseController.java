package io.qdb.server.controller;

import io.qdb.server.JsonService;
import io.qdb.server.model.Database;
import io.qdb.server.model.Repository;
import io.qdb.server.model.User;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

@Singleton
public class DatabaseController extends CrudController {

    private final Repository repo;

    @Inject
    public DatabaseController(Repository repo, JsonService jsonService) {
        super(jsonService);
        this.repo = repo;
    }

    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        User cu = call.getUser();
        ListResult res = new ListResult(offset, limit, repo.findDatabasesVisibleTo(cu, offset, limit));
        if (res.total < 0) res.total = repo.countDatabasesVisibleTo(cu);
        call.setJson(res);
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
    protected void create(Call call) throws IOException {
        if (call.getUser().isAdmin()) {
            call.setJson(repo.createDatabase(getBodyObject(call, Database.class)));
        } else {
            call.setCode(403);
        }
    }

    @Override
    protected Controller getController(Call call, String id, String resource) throws IOException {
        Database db = repo.findDatabase(id);
        if (db != null) {
            if (!db.isVisibleTo(call.getUser())) return StatusCodeController.SC_403;
            if ("queues".equals(resource)) return new QueueController(jsonService, repo, db);
        }
        return StatusCodeController.SC_404;
    }
}
