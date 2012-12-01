package io.qdb.server.controller;

import io.qdb.server.model.Repository;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

/**
 * Database operations.
 */
@Singleton
public class DatabaseController extends CrudController {

    private final Repository repo;

    @Inject
    public DatabaseController(Repository repo) {
        this.repo = repo;
    }

    @Override
    protected void list(Call call) throws IOException {
        call.setJson(repo.findDatabasesVisibleTo(call.getUser()));
    }
}
