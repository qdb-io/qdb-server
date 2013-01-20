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
import java.net.ConnectException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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
    private final ClusterClient.Factory clientFactory;
    private final Paxos<SequenceNo> paxos;

    private Server[] servers;
    private String status;

    @Inject
    public PaxosMasterStrategy(final ScheduledExecutorService pool, final EventBus eventBus, ServerRegistry serverRegistry,
                final OurServer ourServer, final StandaloneRepository local, final ClusterClient.Factory clientFactory) {
        this.pool = pool;
        this.eventBus = eventBus;
        this.serverRegistry = serverRegistry;
        this.ourServer = ourServer;
        this.clientFactory = clientFactory;

        Paxos.SequenceNoFactory<SequenceNo> seqNofactory = new Paxos.SequenceNoFactory<SequenceNo>() {
            @Override
            public SequenceNo next(SequenceNo n) {
                try {
                    return new SequenceNo(local.getNextTxId(), n == null ? 1 : n.seq + 1, ourServer.getId());
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        };

        Paxos.MsgFactory<SequenceNo> msgFactory = new Paxos.MsgFactory<SequenceNo>() {
            @Override
            public Paxos.Msg<SequenceNo> create(Paxos.Msg.Type type, SequenceNo sequenceNo, Object v, SequenceNo nv) {
                return new Msg(type, sequenceNo, (String)v, nv);
            }
        };

        paxos = new Paxos<SequenceNo>(ourServer, new Transport(), seqNofactory, msgFactory, new Paxos.Listener() {
            @Override
            public void accepted(Object v) {
                if (servers != null) {
                    for (Server server : servers) {
                        if (server.getId().equals(v)) {
                            updateStatus(null);
                            eventBus.post(new MasterFound(server));
                            return;
                        }
                    }
                    log.error("Elected master " + v + " not in our server list " + Arrays.asList(servers));
                } else {
                    log.error("Elected master " + v + " but we have no servers?");
                }
            }
        });

        eventBus.register(this);
    }

    @Override
    public String getStatus() {
        return status;
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
            Server[] servers = ev.servers.toArray(new Server[ev.servers.size()]);
            if (this.servers != null && Arrays.equals(this.servers, servers)) return;
            this.servers = servers;
            log.info(updateStatus("Found " + ev.servers));
            paxos.setNodes(servers);
            paxos.propose(ourServer.getId());
            // todo propose needs to have a timeout and retry after a backoff period
        }
    }

    @Subscribe
    public void handleDelivery(Delivery delivery) {
        try {
            paxos.onMessageReceived(new Server(delivery.fromServerId), delivery.msg);
        } catch (Exception e) {
            log.error("Error handling message " + delivery.msg + " from " + delivery.fromServerId + ": " + e, e);
        }
    }

    /**
     * A unique sequence number. The most significant portion is the transaction id of the server generating the number
     * to ensure that only servers with the highest txId (i.e. likely the most up-to-date meta data) can win the master
     * election.
     */
    public static class SequenceNo implements Comparable<SequenceNo> {

        public long txId;
        public int seq;
        public String serverId;

        @SuppressWarnings("UnusedDeclaration")
        public SequenceNo() { }

        public SequenceNo(long txId, int seq, String serverId) {
            this.txId = txId;
            this.seq = seq;
            this.serverId = serverId;
        }

        @Override
        public int compareTo(SequenceNo o) {
            if (txId != o.txId) return txId < o.txId ? -1 : +1;
            if (seq != o.seq) return seq < o.seq ? -1 : +1;
            return serverId.compareTo(o.serverId);
        }

        @Override
        public String toString() {
            String sid;
            if (serverId.startsWith("https://")) sid = serverId.substring(8);
            else if (serverId.startsWith("http://")) sid = serverId.substring(7);
            else sid = serverId;
            if (sid.endsWith("/")) sid = sid.substring(0, sid.length() - 1);
            return Long.toHexString(txId) + "-" + seq + "-" + sid;
        }
    }

    public static class Msg implements Paxos.Msg<SequenceNo> {

        public Type type;
        public SequenceNo n;
        public String v;
        public SequenceNo nv;

        @SuppressWarnings("UnusedDeclaration")
        public Msg() { }

        public Msg(Type type, SequenceNo n, String v, SequenceNo nv) {
            this.type = type;
            this.n = n;
            this.v = v;
            this.nv = nv;
        }

        @Override
        public Type getType() { return type; }

        @Override
        public SequenceNo getN() { return n; }

        @Override
        public Object getV() { return v; }

        @Override
        public SequenceNo getNv() { return nv; }

        @Override
        public String toString() {
            return type + (n != null ? " n=" + n : "" ) + (v != null ? " v=" + v : "") + (nv != null ? " nv=" + nv : "");
        }
    }

    public static class Delivery {
        public final Msg msg;
        public final String fromServerId;

        public Delivery(Msg msg, String fromServerId) {
            this.msg = msg;
            this.fromServerId = fromServerId;
        }
    }

    private class Transport implements Paxos.Transport<SequenceNo> {

        private final Map<Server, ClusterClient> clients = new HashMap<Server, ClusterClient>();

        @Override
        public void send(Object to, final Paxos.Msg<SequenceNo> msg, Object from) {
            Server toServer = (Server)to;
            if (ourServer.equals(toServer)) {
                paxos.onMessageReceived(from, msg);
            } else {
                final ClusterClient client = getClient(toServer);
                pool.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            client.POST("cluster/paxos", msg, null);
                        } catch (ConnectException e) {
                            log.error("Error POSTing to " + client.server + ": " + msg + ": " + e.getMessage());
                        } catch (IOException e) {
                            log.error(e.toString());
                        } catch (Exception e) {
                            log.error("Error POSTing to " + client.server + ": " + e, e);
                        }
                    }
                });
            }
        }

        private synchronized ClusterClient getClient(Server server) {
            ClusterClient client = clients.get(server);
            if (client == null) clients.put(server, client = clientFactory.create(server));
            return client;
        }
    }
}
