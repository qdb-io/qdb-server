package io.qdb.server.controller;

import io.qdb.server.model.Database;
import io.qdb.server.model.Repository;

import java.io.IOException;

/**
 * Queue operations.
 */
public class QueueController extends CrudController {

    private final Repository repo;
    private final Database db;

    public QueueController(Repository repo, Database db) {
        this.repo = repo;
        this.db = db;
    }

    @Override
    protected void list(Call call) throws IOException {
        call.setJson(repo.findQueues(db));
    }
}
