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
        public ServerDTO[] slaves;
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
                    for (int i = 0; i < servers.length; i++) servers[i] = ServerDTO.create(repoStatus.servers[i]);
                }
                if (repoStatus.slaves != null) {
                    slaves = new ServerDTO[repoStatus.slaves.length];
                    for (int i = 0; i < slaves.length; i++) slaves[i] = ServerDTO.create(repoStatus.slaves[i]);
                }
                serverDiscoveryStatus = repoStatus.serverDiscoveryStatus;
                masterElectionStatus = repoStatus.masterElectionStatus;
            }
        }
    }

    public static class ServerDTO {
        public String id;
        public String role;
        public Boolean up;
        public Integer msSinceLastContact;
        public String message;

        public ServerDTO(Server s) {
            id = s.getId();
        }

        public ServerDTO(Repository.ServerStatus s) {
            id = s.id;
            if (s.role != null) role = s.role.name();
            if (s.up) up = true;
            msSinceLastContact = s.msSinceLastContact;
            message = s.message;
        }

        public static ServerDTO create(Server s) {
            return s == null ? null : new ServerDTO(s);
        }

        public static ServerDTO create(Repository.ServerStatus s) {
            return s == null ? null : new ServerDTO(s);
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
        call.setJson(new StatusDTO(id, repo.getStatus(), call.getAuth() != null && !call.getAuth().isAnonymous()));
    }
}
