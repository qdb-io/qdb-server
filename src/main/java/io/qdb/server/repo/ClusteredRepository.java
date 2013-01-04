package io.qdb.server.repo;

import com.google.common.eventbus.EventBus;
import io.qdb.server.ServerId;
import io.qdb.server.model.*;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Date;
import java.util.List;

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
    private final EventBus eventBus;
    private final String serverId;
    private final String clusterName;
    private final String clusterPassword;

    enum State { NOT_CONNECTED, MASTER, SLAVE }

    private Server master;
    private Date upSince;

    @Inject
    public ClusteredRepository(StandaloneRepository local, EventBus eventBus, ServerId serverId,
            @Named("clusterName") String clusterName,
            @Named("clusterPassword") String clusterPassword) throws IOException {
        this.local = local;
        this.eventBus = eventBus;
        this.serverId = serverId.get();
        this.clusterName = clusterName;
        this.clusterPassword = clusterPassword;

        // find out who is supposed to be in the cluster
        List<Server> servers = local.findServers();
        if (servers.isEmpty()) {

        }


        // which nodes are up?
        // who is master?
    }

    private synchronized boolean isMaster() {
        return master != null && serverId.equals(master.getId());
    }

    /**
     * Append tx to our tx log and apply it to our in memory model. This is used to append transactions from slaves.
     * Throws an exception if we are not the master.
     */
    public synchronized void appendTxFromSlave(RepoTx tx) throws IOException, ModelException {
        if (!isMaster()) throw new ClusterException.NotMaster();
        local.exec(tx);
    }

    @Override
    protected synchronized void exec(RepoTx tx) throws IOException, ModelException {
        if (upSince == null) {
            // todo put more detailed info on why repo is not available in here
            throw new UnavailableException("Not available");
        }
        if (isMaster()) {
            local.exec(tx);
        } else {

        }
    }

    @Override
    public Status getStatus() {
        Status s = new Status();
        s.upSince = upSince;
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
}
