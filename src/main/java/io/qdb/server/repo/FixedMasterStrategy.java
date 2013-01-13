package io.qdb.server.repo;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.qdb.server.OurServer;
import io.qdb.server.controller.ServerStatusController;
import io.qdb.server.model.Server;
import io.qdb.server.util.StoppableTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

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
    private final BackoffPolicy pingMasterBackoff;
    private final Server master;

    private List<Server> servers;
    private String status;
    private MasterPinger masterPinger;

    @Inject
    public FixedMasterStrategy(EventBus eventBus, ServerRegistry serverRegistry,
                ScheduledExecutorService executorService, ClusterClient.Factory clientFactory, OurServer ourServer,
                @Named("pingMasterBackoff") BackoffPolicy pingMasterBackoff,
                @Named("master") String master) {
        this.eventBus = eventBus;
        this.serverRegistry = serverRegistry;
        this.executorService = executorService;
        this.clientFactory = clientFactory;
        this.ourServer = ourServer;
        this.pingMasterBackoff = pingMasterBackoff;
        this.master = new Server(master);
        eventBus.register(this);
    }

    @Override
    public void chooseMaster() {
        log.info(updateStatus("Discovering which servers are in the cluster"));
        synchronized (this) {
            this.servers = null;
        }
        serverRegistry.lookForServers();
    }

    @Subscribe
    public void handleServersFound(ServerRegistry.ServersFound ev) {
        synchronized (this) {
            if (servers != null && servers.equals(ev.servers)) return;
            servers = ev.servers;
            log.info(updateStatus("Found " + servers));
        }
        if (!ev.servers.contains(master)) {
            log.warn(updateStatus("master [" + master + "] not in cluster " + servers + " ?"));
            return;
        }

        if (ourServer.equals(master)) {
            postMasterFound();
        } else {
            // check that the master is up before announcing it
            if (masterPinger != null) masterPinger.stop();
            executorService.submit(masterPinger = new MasterPinger());
        }
    }

    private void postMasterFound() {
        updateStatus(null);
        eventBus.post(new MasterFound(master));
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
        if (masterPinger != null) {
            masterPinger.stop();
            masterPinger = null;
        }
    }

    private class MasterPinger extends StoppableTask {

        @Override
        protected void runImpl() {
            updateStatus("Checking that master " + master + " us up");
            for (int errorCount = 1; !isStopped(); pingMasterBackoff.sleep(++errorCount)) {
                try {
                    ServerStatusController.StatusDTO dto = clientFactory.create(master).GET("cluster/status",
                            ServerStatusController.StatusDTO.class);
                    if (dto.up) {
                        postMasterFound();
                        break;
                    }
                    log.info(updateStatus("Master " + master + " is DOWN"));
                } catch (InterruptedIOException ignore) {
                } catch (IOException e) {
                    log.info(updateStatus("Master " + master + " not responding: " + e));
                }
            }
        }
    }
}
