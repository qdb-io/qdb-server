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
    private final Date startTime;

    @Inject
    public ServerStatusController(Repository repo) {
        this.repo = repo;
        startTime = new Date();
    }

    @Override
    public void handle(Call call) throws IOException {
        if (!call.isGet()) {
            call.setCode(400);
            return;
        }
        Repository.Status rs = repo.getStatus();
        Status s = new Status();
        s.startTime = startTime;
        s.status = rs.state;
        s.upSince = rs.upSince;
        call.setJson(s);
    }

    private static class Status {
        public Repository.State status;
        public Date upSince;
        public Date startTime;
    }

}
