package io.qdb.server.repo.cluster;

import io.qdb.server.model.Server;

import java.io.Closeable;

/**
 * Discoveres servers in the cluster.
 */
public interface ServerLocator extends Closeable {

    /**
     * Post an event on the shared event bus when servers have been located.
     */
    public void lookForServers();

    /**
     * Servers have been located.
     */
    public static class ServersFound {

        public final Server[] servers;

        public ServersFound(Server[] servers) {
            this.servers = servers;
        }
    }

}
