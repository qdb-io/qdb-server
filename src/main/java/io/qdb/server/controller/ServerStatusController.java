package io.qdb.server.controller;

import io.qdb.server.ServerId;
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

    private final String id;
    private final Repository repo;

    public static class StatusDTO {

        public String id;
        public Date upSince;
        public boolean up;

        public StatusDTO(String id, Repository.Status repoStatus) {
            this.id = id;
            upSince = repoStatus.upSince;
            up = repoStatus.isUp();
        }
    }

    @Inject
    public ServerStatusController(ServerId serverId, Repository repo) {
        this.id = serverId.get();
        this.repo = repo;
    }

    @Override
    public void handle(Call call) throws IOException {
        if (!call.isGet()) {
            call.setCode(400);
            return;
        }
        call.setJson(new StatusDTO(id, repo.getStatus()));
    }
}
