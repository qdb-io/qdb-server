package io.qdb.server.controller;

import io.qdb.server.JsonService;
import io.qdb.server.model.Database;
import io.qdb.server.model.Queue;
import io.qdb.server.model.Repository;

import java.io.IOException;

public class QueueController extends CrudController {

    private final Repository repo;
    private final Database db;

    public QueueController(JsonService jsonService, Repository repo, Database db) {
        super(jsonService);
        this.repo = repo;
        this.db = db;
    }

    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        call.setJson(repo.findQueues(db));
    }

    @Override
    protected void show(Call call, String id) throws IOException {
        Queue q = repo.findQueue(db, id);
        if (q == null) {
            call.setCode(404);
        } else {
            call.setJson(q);
        }
    }
}
