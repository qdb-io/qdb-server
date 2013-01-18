package io.qdb.server.repo.cluster;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.qdb.server.OurServer;
import io.qdb.server.model.Server;
import io.qdb.server.repo.StandaloneRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Uses the Paxos algorithm to decide who the master is.
 */
@Singleton
public class PaxosMasterStrategy implements MasterStrategy {

    private static final Logger log = LoggerFactory.getLogger(PaxosMasterStrategy.class);

    private final ScheduledExecutorService pool;
    private final EventBus eventBus;
    private final ServerRegistry serverRegistry;
    private final OurServer ourServer;
    private final StandaloneRepository local;

    private List<Server> servers;
    private String status;

    @Inject
    public PaxosMasterStrategy(ScheduledExecutorService pool, EventBus eventBus, ServerRegistry serverRegistry,
                OurServer ourServer, StandaloneRepository local) {
        this.pool = pool;
        this.eventBus = eventBus;
        this.serverRegistry = serverRegistry;
        this.ourServer = ourServer;
        this.local = local;
        eventBus.register(this);
    }

    @Override
    public String getStatus() {
        return null;
    }

    private synchronized String updateStatus(String status) {
        this.status = status;
        return status;
    }

    @Override
    public void close() throws IOException {
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

        // propose ourselves as master
//        try {
//            Prepare prepare = new Prepare();
//            prepare.n = new SequenceNo(local.getNextTxId(), highestSeqNoSeen == null ? 1 : highestSeqNoSeen.seq + 1,
//                    ourServer.getId());
//            prepare.v = ourServer.getId();
//
//        } catch (IOException e) {
//            log.error(e.toString(), e);
//            // todo what to do about this?
//        }
    }


}
