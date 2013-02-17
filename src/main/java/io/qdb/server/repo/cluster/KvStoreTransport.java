package io.qdb.server.repo.cluster;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.qdb.kvstore.cluster.Transport;
import io.qdb.server.OurServer;
import io.qdb.server.model.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Sends messages to servers in our cluster for kvstore.
 */
@Singleton
public class KvStoreTransport implements Transport {

    private static final Logger log = LoggerFactory.getLogger(KvStoreTransport.class);

    private final OurServer ourServer;
    private final ServerLocator serverLocator;
    private final ExecutorService executorService;
    private final ClusterClient.Factory clientFactory;

    private Listener listener;
    private Map<String, ClusterClient> clients = new HashMap<String, ClusterClient>();

    @Inject
    public KvStoreTransport(OurServer ourServer, ServerLocator serverLocator, ExecutorService executorService,
                            EventBus eventBus, ClusterClient.Factory clientFactory) {
        this.ourServer = ourServer;
        this.serverLocator = serverLocator;
        this.executorService = executorService;
        this.clientFactory = clientFactory;
        eventBus.register(this);
    }

    @Override
    public void init(Listener listener) {
        this.listener = listener;
    }

    @Subscribe
    public void onServersFound(ServerLocator.ServersFound ev) {
        String[] a = new String[ev.servers.length];
        for (int i = 0; i < a.length; i++) a[i] = ev.servers[i].getId();
        listener.onServersFound(a);
    }

    @Override
    public String getSelf() {
        return ourServer.getId();
    }

    @Override
    public void lookForServers() {
        serverLocator.lookForServers();
    }

    @Override
    public void send(final String to, final byte[] msg) {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                sendImpl(to, msg);
            }
        });
    }

    @Override
    public byte[] get(String from, byte[] msg) throws IOException {
        return getClient(from).send("/cluster/kvstore", msg);
    }

    private synchronized ClusterClient getClient(String serverId) {
        ClusterClient ans = clients.get(serverId);
        if (ans == null) clients.put(serverId, ans = clientFactory.create(new Server(serverId)));
        return ans;
    }

    private void sendImpl(final String to, byte[] msg) {
        ClusterClient client = getClient(to);
        try {
            client.send("/cluster/kvstore", msg);
        } catch (IOException e) {
            log.error("Error POSTing " + msg.length + " byte(s) to " + to + ": " + e, e);
        }
    }

    /**
     * This is called by {@link io.qdb.server.controller.cluster.KvStoreController} when it receives a kvstore
     * message from another server in the cluster.
     */
    public void onMessageReceived(String from, InputStream ins, OutputStream out) throws IOException {
        listener.onMessageReceived(from, ins, out);
    }

    @Override
    public void close() throws IOException {
        serverLocator.close();
        Map<String, ClusterClient> copy;
        synchronized (this) {
            copy = clients;
            clients = null;
        }
        if (copy != null) {
            for (Map.Entry<String, ClusterClient> e : copy.entrySet()) {
                try {
                    e.getValue().close();
                } catch (Exception x) {
                    if (log.isDebugEnabled()) log.debug(x.toString(), x);
                }
            }
        }
    }
}