package io.qdb.server.repo;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.qdb.server.model.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.List;

/**
 * The master is specified in the configuration.
 */
@Singleton
public class FixedMasterStrategy implements MasterStrategy {

    private static final Logger log = LoggerFactory.getLogger(FixedMasterStrategy.class);

    private final EventBus eventBus;
    private final ServerRegistry serverRegistry;
    private final Server master;

    private List<Server> servers;

    @Inject
    public FixedMasterStrategy(EventBus eventBus, ServerRegistry serverRegistry, @Named("master") String master) {
        this.eventBus = eventBus;
        this.serverRegistry = serverRegistry;
        this.master = new Server(master);
        eventBus.register(this);
    }

    @Override
    public void chooseMaster() {
        synchronized (this) {
            servers = null;
        }
        serverRegistry.lookForServers();
    }

    @Subscribe
    public void handleServersFound(ServerRegistry.ServersFound ev) {
        if (log.isDebugEnabled()) log.debug(ev.toString());
        synchronized (this) {
            if (servers != null && servers.equals(ev.servers)) return;
            servers = ev.servers;
        }
        if (!ev.servers.contains(master)) {
            log.warn("master [" + master + "] not in cluster " + servers + " ?");
            return;
        }
        // todo check master responds to pings
        log.info("Cluster is " + ev.servers + " master " + master);
        eventBus.post(new MasterFound(master));
    }

    @Override
    public String getStatus() {
        return null;
    }

    @Override
    public void close() throws IOException {
    }
}
