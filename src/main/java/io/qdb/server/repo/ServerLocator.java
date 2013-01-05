package io.qdb.server.repo;

import io.qdb.server.OurServer;
import io.qdb.server.model.Server;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.List;

/**
 * Figures out what servers are in our cluster.
 */
@Singleton
public class ServerLocator {

    private final StandaloneRepository local;
    private final OurServer ourServer;
    private final String[] configuredServerURLs;

    @Inject
    public ServerLocator(StandaloneRepository local, OurServer ourServer,
                @Named("servers") String servers) {
        this.local = local;
        this.ourServer = ourServer;

        if (servers != null && servers.trim().length() > 0) {
            String[] a = servers.split("[\\w]*,[\\w]*");
            configuredServerURLs = new String[a.length];
            for (int i = 0; i < a.length; i++) {
                try {
                    configuredServerURLs[i] = Server.cleanURL(a[i]);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("servers property contains bad entry: " + e.getMessage());
                }
            }
        } else {
            configuredServerURLs = new String[0];
        }
    }

    /**
     * Get the the servers known to be in our cluster including ourselves. Note that these might not contain ids.
     */
    public List<Server> getServers() {
        return null;
    }
}
