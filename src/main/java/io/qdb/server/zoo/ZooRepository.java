package io.qdb.server.zoo;

import io.qdb.server.model.Database;
import io.qdb.server.model.Queue;
import io.qdb.server.model.Repository;
import io.qdb.server.model.User;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.List;

/**
 * Keeps our meta data in ZooKeeper.
 */
@Singleton
public class ZooRepository implements Repository, Watcher {

    private static final Logger log = LoggerFactory.getLogger(ZooRepository.class);

    private final String connectString;
    private final int sessionTimeout;
    private final String root;

    private final ZooKeeper zk;
    private Event.KeeperState zkState;

    @Inject
    public ZooRepository(
                @Named("zookeeper.connectString") String connectString,
                @Named("zookeeper.sessionTimeout") int sessionTimeout,
                @Named("clusterName") String clusterName)
            throws IOException {
        this.connectString = connectString;
        this.sessionTimeout = sessionTimeout;
        this.root = "/qdb/" + clusterName;
        log.info("Connecting to ZooKeeper(s) [" + connectString + "] sessionTimeout " + sessionTimeout);
        zk = new ZooKeeper(connectString, sessionTimeout, this);
    }

    @Override
    public synchronized void process(WatchedEvent event) {
        try {
            log.debug(event.toString());
            zkState = event.getState();
            if (zkState == Event.KeeperState.Disconnected) {
                log.info("Disconnected from ZooKeeper");
            } else if (zkState == Event.KeeperState.SyncConnected) {
                log.info("Connected to ZooKeeper");
                createRoot();
//                serverInfo.publish();
            } else if (zkState == Event.KeeperState.ConnectedReadOnly) {
                log.info("Connected to ZooKeeper in read-only mode");
            }
        } catch (Exception e) {
            log.error(e.toString(), e);
        }
    }

    private void createRoot() throws KeeperException, InterruptedException {
        try {
            zk.create("/qdb", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ignore) {
        }
        try {
            zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ignore) {
        }
    }

    @Override
    public synchronized Status getStatus() throws IOException {
        if (zkState == Event.KeeperState.SyncConnected) return Status.CONNECTED;
        if (zkState == Event.KeeperState.ConnectedReadOnly) return Status.CONNECTED_READ_ONLY;
        return Status.DISCONNECTED;
    }

    @Override
    public User findUser(String id) {
        return null;
    }

    @Override
    public List<Database> findDatabasesVisibleTo(User user) {
        return null;
    }

    @Override
    public Database findDatabase(String id) {
        return null;
    }

    @Override
    public List<Queue> findQueues(Database db) {
        return null;
    }

    @Override
    public Queue findQueue(Database db, String nameOrId) {
        return null;
    }
}
