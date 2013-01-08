package io.qdb.server.repo;

import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.EventBus;
import io.qdb.server.OurServer;
import io.qdb.server.model.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.*;

/**
 * All servers in the cluster are specified in the configuration.
 */
@Singleton
public class FixedServerRegistry implements ServerRegistry {

    private static Logger log = LoggerFactory.getLogger(FixedServerRegistry.class);

    private final EventBus eventBus;
    private final List<Server> servers;

    @SuppressWarnings("unchecked")
    @Inject
    public FixedServerRegistry(EventBus eventBus, OurServer ourServer, @Named("servers") String servers) {
        this.eventBus = eventBus;

        if (servers != null && servers.trim().length() > 0) {
            Set<Server> set = new HashSet<Server>();
            for (String url : servers.split("[\\s]*,[\\s]*")) {
                Server server;
                try {
                    server = new Server(url);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("servers property contains bad entry: " + e.getMessage());
                }
                if (!set.add(server)) {
                    throw new IllegalArgumentException("servers property contains duplicate: [" + url + "]");
                }
            }
            if (!set.contains(ourServer)) {
                throw new IllegalArgumentException("servers property does not contain this server: " + ourServer);
            }
            this.servers = ImmutableList.copyOf(set);
        } else {
            this.servers = ImmutableList.of((Server) ourServer);
        }
    }

    @Override
    public void lookForServers() {
        eventBus.post(new ServersFound(servers));
    }

    @Override
    public String getStatus() {
        return null;    // nothing to report - we always know our servers
    }

    @Override
    public void close() throws IOException {
        // nothing to be done
    }
}
