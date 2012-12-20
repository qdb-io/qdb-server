package io.qdb.server.zk;

import com.google.common.eventbus.EventBus;
import com.netflix.curator.CuratorZookeeperClient;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.utils.EnsurePath;
import io.qdb.server.JsonService;
import io.qdb.server.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Keeps our meta data in ZooKeeper.
 */
@Singleton
public class ZkRepository implements Repository, Closeable, ConnectionStateListener {

    private static final Logger log = LoggerFactory.getLogger(ZkRepository.class);

    private final EventBus eventBus;
    private final JsonService jsonService;
    private final String root;
    private final CuratorFramework client;
    private final String initialAdminPassword;

    private Date upSince;
    private ZkModelCache<User> usersCache;
    private ZkModelCache<Database> databasesCache;

    @Inject
    public ZkRepository(EventBus eventBus, JsonService jsonService,
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
        CuratorFramework cf = CuratorFrameworkFactory.newClient(connectString, new ExponentialBackoffRetry(1000, 3));
        cf.getConnectionStateListenable().addListener(this);
        cf.start();
        client = cf.usingNamespace(root);
    }

    @Override
    public void close() throws IOException {
        usersCache.close();
        databasesCache.close();
        client.close();
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        switch (newState) {
            case CONNECTED:
                try {
                    synchronized (this) {
                        CuratorZookeeperClient zk = client.getZookeeperClient();
                        new EnsurePath(root).ensure(zk);
                        new EnsurePath(root + "/nodes").ensure(zk);
                        new EnsurePath(root + "/databases").ensure(zk);
                        new EnsurePath(root + "/queues").ensure(zk);
                        new EnsurePath(root + "/users").ensure(zk);

                        usersCache = new ZkModelCache<User>(User.class, jsonService, this.client, "/users");
                        databasesCache = new ZkModelCache<Database>(Database.class, jsonService, this.client, "/databases");

                        ensureAdminUser();

                        upSince = new Date(); // only set this after zoo has been populated so we know if that failed
                    }
                } catch (Exception e) {
                    log.error("Error initializing ZooKeeper: " + e, e);
                }
                break;
            case RECONNECTED:
                synchronized (this) {
                    upSince = new Date();
                }
                break;
            default:
                synchronized (this) {
                    upSince = null;
                }
                break;
        }
        eventBus.post(getStatus());
    }

    private void ensureAdminUser() throws Exception {
        if (usersCache.find("admin") == null) {
            User admin = new User();
            admin.setId("admin");
            admin.setPassword(initialAdminPassword);
            admin.setAdmin(true);
            usersCache.create(admin);
            log.info("Created initial admin user");
        }
    }

    @Override
    public synchronized Status getStatus() {
        Status ans = new Status();
        ans.upSince = upSince;
        return ans;
    }

    private void checkUp() throws IOException {
        if (upSince == null) throw new UnavailableException("Not connected to ZooKeeper");
    }

    @Override
    public void createQdbNode(QdbNode node) throws IOException {
        checkUp();
    }

    @Override
    public List<QdbNode> findQdbNodes() throws IOException {
        checkUp();
        return null;
    }

    @Override
    public User findUser(String id) throws IOException {
        checkUp();
        return usersCache.find(id);
    }

    @Override
    public User createUser(User user) throws IOException {
        checkUp();
        return usersCache.create(user);
    }

    @Override
    public User updateUser(User user) throws IOException {
        checkUp();
        return usersCache.update(user);
    }

    @Override
    public List<User> findUsers(int offset, int limit) throws IOException {
        checkUp();
        return usersCache.list(offset, limit);
    }

    @Override
    public int countUsers() throws IOException {
        checkUp();
        return usersCache.size();
    }

    @Override
    public List<Database> findDatabasesVisibleTo(User user, int offset, int limit) throws IOException {
        checkUp();
        if (user.isAdmin()) {
            return databasesCache.list(offset, limit);
        } else {
            ArrayList<Database> ans = new ArrayList<Database>();
            String[] databases = user.getDatabases();
            if (databases != null) {
                for (int i = 0, n = Math.min(offset + limit, databases.length); i < n; i++) {
                    Database db = findDatabase(databases[i]);
                    if (db != null) ans.add(db);
                }
            }
            return ans;
        }
    }

    @Override
    public Database findDatabase(String id) throws IOException {
        checkUp();
        return databasesCache.find(id);
    }

    @Override
    public Database createDatabase(Database database) throws IOException {
        checkUp();
        return databasesCache.create(database);
    }

    @Override
    public int countDatabasesVisibleTo(User user) throws IOException {
        checkUp();
        if (user.isAdmin()) {
            return databasesCache.size();
        } else {
            String[] databases = user.getDatabases();
            return databases == null ? 0 : databases.length;
        }
    }

    @Override
    public List<Queue> findQueues(Database db) throws IOException {
        checkUp();
        return null;
    }

    @Override
    public Queue findQueue(Database db, String nameOrId) throws IOException {
        checkUp();
        return null;
    }
}
