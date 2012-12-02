package io.qdb.server.zoo;

import com.google.common.eventbus.EventBus;
import io.qdb.server.JsonService;
import io.qdb.server.model.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Keeps our meta data in ZooKeeper.
 */
@Singleton
public class ZooRepository implements Repository, Watcher {

    private static final Logger log = LoggerFactory.getLogger(ZooRepository.class);

    private final EventBus eventBus;
    private final JsonService jsonService;
    private final String root;
    private final ZooKeeper zk;
    private final String initialAdminPassword;

    private Status status = new Status();

    @Inject
    public ZooRepository(EventBus eventBus, JsonService jsonService,
                         @Named("zookeeper.connectString") String connectString,
                         @Named("zookeeper.sessionTimeout") int sessionTimeout,
                         @Named("clusterName") String clusterName,
                         @Named("initialAdminPassword") String initialAdminPassword)
            throws IOException {
        this.eventBus = eventBus;
        this.jsonService = jsonService;
        this.root = "/qdb/" + clusterName;
        this.initialAdminPassword = initialAdminPassword;
        log.info("Connecting to ZooKeeper(s) [" + connectString + "] sessionTimeout " + sessionTimeout);
        zk = new ZooKeeper(connectString, sessionTimeout, this);
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            synchronized (this) {
                log.debug(event.toString());
                Status ns = new Status();
                Event.KeeperState state = event.getState();
                if (state == Event.KeeperState.Disconnected) {
                    log.info("Disconnected from ZooKeeper");
                    ns.state = State.DOWN;
                } else if (state == Event.KeeperState.SyncConnected) {
                    log.info("Connected to ZooKeeper");
                    ns.state = State.UP;
                    ns.upSince = new Date();
                    populateZoo();
                } else if (state == Event.KeeperState.ConnectedReadOnly) {
                    log.info("Connected to ZooKeeper in read-only mode");
                    ns.state = State.READ_ONLY;
                }
                status = ns;
            }
            eventBus.post(status);
        } catch (Exception e) {
            log.error(e.toString(), e);
        }
    }

    private void populateZoo() throws KeeperException, InterruptedException, IOException {
        ensureNodeExists("/qdb");
        ensureNodeExists(root);
        ensureNodeExists(root + "/nodes");
        ensureNodeExists(root + "/databases");
        ensureNodeExists(root + "/queues");
        ensureNodeExists(root + "/users");
        if (findUser("admin") == null) {
            User admin = new User();
            admin.setId("admin");
            admin.setPasswordHash(initialAdminPassword);
            admin.setAdmin(true);
            createUser(admin);
            log.info("Created initial admin user");
        }
    }

    private boolean ensureNodeExists(String path) throws KeeperException, InterruptedException {
        try {
            zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            return true;
        } catch (KeeperException.NodeExistsException ignore) {
            return false;
        }
    }

    @Override
    public synchronized Status getStatus() {
        return status;
    }

    @Override
    public void create(Node node) throws IOException {
    }

    @Override
    public List<Node> findNodes() throws IOException {
        return null;
    }

    @Override
    public User findUser(String id) throws IOException {
        try {
            return jsonService.fromJson(zk.getData(root + "/users/" + id, false, new Stat()), User.class);
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NONODE) return null;
            throw new IOException(e.toString(), e);
        } catch (InterruptedException e) {
            throw new IOException(e.toString(), e);
        }
    }

    @Override
    public void createUser(User user) throws IOException {
        assert user.getId() != null;
        try {
            zk.create(root + "/users/" + user.getId(), jsonService.toJson(user),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            throw new IOException(e.toString(), e);
        } catch (InterruptedException e) {
            throw new IOException(e.toString(), e);
        }
    }

    private String getLastPart(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
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
