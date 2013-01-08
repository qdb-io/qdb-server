package io.qdb.server.controller;

import io.qdb.server.OurServer;
import io.qdb.server.model.Repository;
import io.qdb.server.model.Server;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Date;

/**
 * Status of the server.
 */
@Singleton
public class ServerStatusController implements Controller {

    private final String id;
    private final Repository repo;

    public static class StatusDTO {

        public String id;
        public Date upSince;
        public boolean up;
        public String clusterName;
        public String serverDiscoveryStatus;
        public String masterElectionStatus;
        public ServerDTO master;
        public ServerDTO[] servers;

        @SuppressWarnings("UnusedDeclaration")
        public StatusDTO() { }

        public StatusDTO(String id, Repository.Status repoStatus, boolean complete) {
            this.id = id;
            upSince = repoStatus.upSince;
            up = repoStatus.isUp();
            clusterName = repoStatus.clusterName;
            if (complete) {
                master = ServerDTO.create(repoStatus.master);
                if (repoStatus.servers != null) {
                    servers = new ServerDTO[repoStatus.servers.length];
                    for (int i = 0; i < servers.length; i++) {
                        servers[i] = ServerDTO.create(repoStatus.servers[i]);
                    }
                }
                serverDiscoveryStatus = repoStatus.serverDiscoveryStatus;
                masterElectionStatus = repoStatus.masterElectionStatus;
            }
        }
    }

    public static class ServerDTO {
        public String id;
        public ServerDTO(Server s) { id = s.getId(); }
        public static ServerDTO create(Server s) { return s == null ? null : new ServerDTO(s); }
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
        call.setJson(new StatusDTO(id, repo.getStatus(), call.getAuth() != null && !call.getAuth().isAnonymous()));
    }
}
