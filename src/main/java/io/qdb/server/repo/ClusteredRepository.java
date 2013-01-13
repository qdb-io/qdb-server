package io.qdb.server.repo;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.qdb.buffer.MessageCursor;
import io.qdb.server.OurServer;
import io.qdb.server.controller.MessageController;
import io.qdb.server.model.*;
import io.qdb.server.model.Queue;
import io.qdb.server.util.StoppableTask;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.*;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Repository implementation for clustered deployment. All repository updates are carried out by the leader of the
 * cluster and the transactions replicated to the other nodes. So slave nodes can be a little out of sync but
 * optimistic locking ensures that this does not cause any trouble.
 *
 * Leader election?
 *
 * Cluster membership?
 */
@Singleton
public class ClusteredRepository extends RepositoryBase {

    private final StandaloneRepository local;
    private final ServerRegistry serverRegistry;
    private final MasterStrategy masterStrategy;
    private final ClusterClient.Factory clientFactory;
    private final EventBus eventBus;
    private final OurServer ourServer;
    private final ScheduledExecutorService executorService;
    private final JsonConverter jsonConverter;
    private final SlaveRegistry slaveRegistry;
    private final BackoffPolicy slaveTxDownloadBackoff;
    private final BackoffPolicy slaveTxExecBackoff;
    private final String clusterName;

    private Server[] servers;
    private ClusterClient master;
    private Date upSince;

    private TxDownloader txDownloader;

    @Inject
    public ClusteredRepository(StandaloneRepository local, EventBus eventBus, OurServer ourServer,
               ServerRegistry serverRegistry, MasterStrategy masterStrategy, ClusterClient.Factory clientFactory,
               ScheduledExecutorService executorService, JsonConverter jsonConverter, SlaveRegistry slaveRegistry,
               @Named("clusterName") String clusterName,
               @Named("slaveTxDownloadBackoff") BackoffPolicy slaveTxDownloadBackoff,
               @Named("slaveTxExecBackoff") BackoffPolicy slaveTxExecBackoff
               ) throws IOException {
        this.local = local;
        this.eventBus = eventBus;
        this.serverRegistry = serverRegistry;
        this.masterStrategy = masterStrategy;
        this.clientFactory = clientFactory;
        this.ourServer = ourServer;
        this.executorService = executorService;
        this.jsonConverter = jsonConverter;
        this.slaveRegistry = slaveRegistry;
        this.clusterName = clusterName;
        this.slaveTxDownloadBackoff = slaveTxDownloadBackoff;
        this.slaveTxExecBackoff = slaveTxExecBackoff;

        eventBus.register(this);
        masterStrategy.chooseMaster();
    }

    @Override
    public void close() throws IOException {
        closeQuietly(local);
        closeQuietly(masterStrategy);
        closeQuietly(serverRegistry);
    }

    private void closeQuietly(Closeable o) {
        try {
            o.close();
        } catch (IOException e) {
            log.warn(e.toString(), e);
        }
    }

    @Subscribe
    public void handleServersFound(ServerRegistry.ServersFound ev) {
        try {
            synchronized (this) {
                this.servers = ev.servers.toArray(new Server[ev.servers.size()]);
            }
        } catch (Exception e) {
            log.error(e.toString(), e);
        }
    }

    @Subscribe
    public void handleMasterFound(MasterStrategy.MasterFound ev) {
        try {
            synchronized (this) {
                if (master != null && master.server.equals(ev.master)) return;

                master = clientFactory.create(ev.master);
                if (isMaster()) {
                    log.info("We are the MASTER " + master);
                } else {
                    log.info("We are a SLAVE, master is " + master);
                }

                if (isSlave()) {
                    // get our snapshot up to date with our tx log
                    local.saveSnapshot();
                    // todo get local snapshot in sync with master (may need merge + reset of our tx log to master id etc.)
                    executorService.execute(txDownloader = new TxDownloader());
                }

                upSince = new Date();
            }
            eventBus.post(getStatus());
        } catch (Exception e) {
            log.error(e.toString(), e);
        }
    }

    private synchronized boolean isMaster() {
        return master != null && ourServer.equals(master.server);
    }

    private synchronized boolean isSlave() {
        return master != null && !ourServer.equals(master.server);
    }

    private void chooseMaster() {
        synchronized (this) {
            upSince = null;
            master = null;
            if (txDownloader != null) {
                txDownloader.stop();
                txDownloader = null;
            }
            slaveRegistry.disconnectAndClear();
        }
        eventBus.post(getStatus());
        masterStrategy.chooseMaster();
    }

    /**
     * Append tx to our tx log and apply it to our in memory model. This is used to append transactions from slaves.
     * Throws an exception if we are not the master. Returns the transaction id to be sent back to the client.
     */
    public synchronized long appendTxFromSlave(RepoTx tx) throws IOException, ModelException {
        if (!isMaster()) throw new ClusterException.NotMaster();
        return local.exec(tx);
    }

    /**
     * Open a cursor to our tx log for slaves to receive transactions.
     */
    public synchronized MessageCursor openTxCursor(long id) throws IOException {
        if (!isMaster()) throw new ClusterException.NotMaster();
        return local.openTxCursor(id);
    }

    @Override
    protected long exec(RepoTx tx) throws IOException, ModelException {
        synchronized (this) {
            if (upSince == null) throw new UnavailableException("Not available"); // todo more detailed status
            if (isMaster()) return local.exec(tx);
        }

        StandaloneRepository.TxMonitor txMonitor = local.createTxMonitor();
        try {
            long id;
            try {
                id = master.POST("cluster/transactions", tx, TxId.class).id;
            } catch (ResponseCodeException e) {
                if (e.responseCode == 409) throw new ModelException(e.text);
                log.error(e.getMessage());
                if (e.responseCode == 410) chooseMaster();
                throw e;
            }
            if (txMonitor.waitFor(id, master.getTimeoutMs())) return id;
            String msg = "Timeout waiting for tx " + id + " from master " + master;
            log.error(msg);
            chooseMaster();
            throw new ClusterException.MasterTimeout(msg);
        } finally {
            try {
                txMonitor.close();
            } catch (IOException e) {
                log.warn("Error closing txMonitor: " + e);
            }
        }
    }

    @Override
    public synchronized Status getStatus() {
        Status s = new Status();
        s.upSince = upSince;
        s.clusterName = clusterName;
        if (master != null) {
            s.master = master.getStatus();
            s.master.role = ServerRole.MASTER;
            if (isMaster()) s.master.msSinceLastContact = 0;
        }
        s.slaves = slaveRegistry.getSlaveStatuses();
        s.servers = servers;
        s.serverDiscoveryStatus = serverRegistry.getStatus();
        s.masterElectionStatus = masterStrategy.getStatus();
        return s;
    }

    @Override
    public List<Server> findServers() throws IOException {
        return local.findServers();
    }

    @Override
    public User findUser(String id) throws IOException {
        return local.findUser(id);
    }

    @Override
    public List<User> findUsers(int offset, int limit) throws IOException {
        return local.findUsers(offset, limit);
    }

    @Override
    public int countUsers() throws IOException {
        return local.countUsers();
    }

    @Override
    public Database findDatabase(String id) throws IOException {
        return local.findDatabase(id);
    }

    @Override
    public List<Database> findDatabasesVisibleTo(User user, int offset, int limit) throws IOException {
        return local.findDatabasesVisibleTo(user, offset, limit);
    }

    @Override
    public int countDatabasesVisibleTo(User user) throws IOException {
        return local.countDatabasesVisibleTo(user);
    }

    @Override
    public Queue findQueue(String id) throws IOException {
        return local.findQueue(id);
    }

    @Override
    public List<Queue> findQueues(int offset, int limit) throws IOException {
        return local.findQueues(offset, limit);
    }

    @Override
    public int countQueues() throws IOException {
        return local.countQueues();
    }

    /**
     * Streams transactions from the master to us when we are acting as a slave.
     */
    private class TxDownloader extends StoppableTask {

        public synchronized ClusterClient getMaster() {
            return isStopped() ? null : master;
        }

        protected void runImpl() {
            int ioExceptionCount = 0;
            int execFailureCount = 0;
            Server lastMaster = null;
            boolean chooseMaster = false;

            for (ClusterClient m; (m = getMaster()) != null; ) {
                if (lastMaster == null) {
                    lastMaster = m.server;
                } else if (!lastMaster.equals(m.server)) {
                    log.warn("Master changed from " + lastMaster + " to " + m.server + " without stopping " + this);
                    break;
                }

                Exception execException = null;
                try {
                    if (log.isDebugEnabled()) log.debug("Connecting to master " + lastMaster);
                    PushbackInputStream ins = new PushbackInputStream(
                            m.GET("cluster/transactions?txId=" + local.getNextTxId()), 1);
                    try {
                        while (getMaster() != null) {
                            while (true) {
                                int b = ins.read();
                                ioExceptionCount = 0;
                                master.updateLastContact();
                                if (b != 10) {
                                    ins.unread(b);
                                    break;
                                }
                            }

                            MessageController.MessageHeader h = jsonConverter.readValue(ins,
                                    MessageController.MessageHeader.class);
                            RepoTx tx = jsonConverter.readValue(ins, RepoTx.class);

                            try {
                                if (log.isDebugEnabled()) log.debug("Executing txId " + h.id + " from master " + m.server);
                                local.exec(tx);
                            } catch (ModelException e) {
                                log.warn("Tx from master failed: " + e);
                            } catch (Exception e) {
                                log.error("Error executing tx from master: " + e, e);
                                execException = e;
                                break;
                            }
                        }
                    } finally {
                        closeQuietly(ins);
                    }
                } catch (InterruptedIOException e) {
                    if (log.isDebugEnabled()) log.debug(e.toString());
                } catch (IOException e) {
                    if (e instanceof ResponseCodeException && ((ResponseCodeException)e).responseCode == 410) {
                        log.info("Got 410 from master " + m + " (longer the master)");
                        chooseMaster = true;
                        break;
                    }
                    int ms = slaveTxDownloadBackoff.getMaxDelayMs();
                    long lastContact = master.getLastContact();
                    if (lastContact > 0) ms -= (int)(System.currentTimeMillis() - lastContact);
                    if (ms > 0) {
                        log.error("Error streaming transactions from master " + m + ", retrying: " + e);
                        slaveTxDownloadBackoff.sleep(++ioExceptionCount, ms);
                    } else {
                        log.error("Error streaming transactions from master " + m + ": " + e);
                        chooseMaster = true;
                        break;
                    }
                }

                if (execException != null) {
                    slaveTxExecBackoff.sleep(++execFailureCount);
                } else {
                    execFailureCount = 0;
                }
            }

            if (lastMaster == null) {
                if (log.isDebugEnabled()) log.debug(this + " exiting without connecting to master");
            } else {
                if (log.isDebugEnabled()) log.debug("Disconnected from master " + lastMaster);
            }

            if (chooseMaster) {
                log.info("Master timeout " + slaveTxDownloadBackoff.getMaxDelayMs() + " ms exceed, choosing new master");
                chooseMaster();
            }
        }
    }
}
