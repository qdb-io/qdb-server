package io.qdb.server.repo;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.qdb.server.OurServer;
import io.qdb.server.controller.ServerStatusController;
import io.qdb.server.model.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The master is specified in the configuration.
 */
@Singleton
public class FixedMasterStrategy implements MasterStrategy {

    private static final Logger log = LoggerFactory.getLogger(FixedMasterStrategy.class);

    private final EventBus eventBus;
    private final ServerRegistry serverRegistry;
    private final ScheduledExecutorService executorService;
    private final ClusterClient.Factory clientFactory;
    private final OurServer ourServer;
    private final Server master;

    private final Callable<Void> pingCommand  = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
            pingMaster();
            return null;
        }
    };

    private List<Server> servers;
    private String status;

    @Inject
    public FixedMasterStrategy(EventBus eventBus, ServerRegistry serverRegistry,
                ScheduledExecutorService executorService, ClusterClient.Factory clientFactory, OurServer ourServer,
                @Named("master") String master) {
        this.eventBus = eventBus;
        this.serverRegistry = serverRegistry;
        this.executorService = executorService;
        this.clientFactory = clientFactory;
        this.ourServer = ourServer;
        this.master = new Server(master);
        eventBus.register(this);
    }

    @Override
    public void chooseMaster() {
        updateStatus("Discovering which servers are in the cluster");
        serverRegistry.lookForServers();
    }

    @Subscribe
    public void handleServersFound(ServerRegistry.ServersFound ev) {
        synchronized (this) {
            if (servers != null && servers.equals(ev.servers)) return;
            servers = ev.servers;
            updateStatus("Found " + servers);
        }
        if (!ev.servers.contains(master)) {
            log.warn("master [" + master + "] not in cluster " + servers + " ?");
            return;
        }

        if (ourServer.equals(master)) {
            postMasterFound();
        } else {
            // check that the master is up before announcing it
            executorService.submit(pingCommand);
        }
    }

    private void postMasterFound() {
        updateStatus("Master found");
        eventBus.post(new MasterFound(master));
    }

    private void pingMaster() {
        updateStatus("Checking that master " + master + " us up");
        try {
            ServerStatusController.StatusDTO dto = clientFactory.create(master).GET("cluster/status",
                    ServerStatusController.StatusDTO.class);
            if (dto.up) {
                postMasterFound();
                return;
            }
            log.info(updateStatus("Master " + master + " is DOWN"));
        } catch (IOException e) {
            log.info(updateStatus("Master " + master + " not responding: " + e));
        }
        executorService.schedule(pingCommand, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized String getStatus() {
        return status;
    }

    private synchronized String updateStatus(String status) {
        this.status = status;
        return status;
    }

    @Override
    public void close() throws IOException {
    }
}
