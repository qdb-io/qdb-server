package io.qdb.server.controller;

import io.qdb.server.model.Repository;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Date;

/**
 * Basic status of the server.
 */
@Singleton
public class ServerStatusController implements Controller {

    private final Repository repo;

    @Inject
    public ServerStatusController(Repository repo) {
        this.repo = repo;
    }

    @Override
    public void handle(Call call) throws IOException {
        if (!call.isGet()) {
            call.setCode(400);
            return;
        }
        call.setJson(repo.getStatus());
    }
}
