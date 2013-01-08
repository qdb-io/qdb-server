package io.qdb.server.controller;

import io.qdb.server.OurServer;
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

        @SuppressWarnings("UnusedDeclaration")
        public StatusDTO() { }

        public StatusDTO(String id, Repository.Status repoStatus) {
            this.id = id;
            upSince = repoStatus.upSince;
            up = repoStatus.isUp();
        }
    }

    @Inject
    public ServerStatusController(OurServer ourServer, Repository repo) {
        this.id = ourServer.getId();
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
