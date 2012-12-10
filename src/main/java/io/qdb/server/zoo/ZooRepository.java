package io.qdb.server.zoo;

import com.google.common.eventbus.EventBus;
import com.netflix.curator.CuratorZookeeperClient;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ZKPaths;
import io.qdb.server.JsonService;
import io.qdb.server.model.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.common.PathUtils;
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
public class ZooRepository implements Repository, Closeable, ConnectionStateListener {

    private static final Logger log = LoggerFactory.getLogger(ZooRepository.class);

    private final EventBus eventBus;
    private final JsonService jsonService;
    private final String root;
    private final CuratorFramework client;
    private final String initialAdminPassword;

    private Date upSince;
    private PathChildrenCache usersCache;

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
        CuratorFramework cf = CuratorFrameworkFactory.newClient(connectString, new ExponentialBackoffRetry(1000, 3));
        cf.getConnectionStateListenable().addListener(this);
        cf.start();
        client = cf.usingNamespace(root);
    }

    @Override
    public void close() throws IOException {
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

                        usersCache = new PathChildrenCache(this.client, "/users", true);
                        usersCache.start(true);

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
        if (findUser("admin") == null) {
            User admin = new User();
            admin.setId("admin");
            admin.setPasswordHash(initialAdminPassword);
            admin.setAdmin(true);
            createUser(admin);
            log.info("Created initial admin user");
        }
    }

    @Override
    public synchronized Status getStatus() {
        Status ans = new Status();
        ans.upSince = upSince;
        return ans;
    }

    @Override
    public void createQdbNode(QdbNode node) throws IOException {
    }

    @Override
    public List<QdbNode> findQdbNodes() throws IOException {
        return null;
    }

    @Override
    public User findUser(String id) throws IOException {
        try {
            return jsonService.fromJson(client.getData().forPath("/users/" + id), User.class);
        } catch (KeeperException.NoNodeException ignore) {
            return null;
        } catch (Exception e) {
            throw new IOException(e.toString(), e);
        }
    }

    @Override
    public void createUser(User user) throws IOException {
        assert user.getId() != null;
        try {
            User u = (User)user.clone();
            u.setId(null);
            client.create().forPath("/users/" + user.getId(), jsonService.toJson(u));
        } catch (KeeperException.NodeExistsException e) {
            throw new ModelException("User [" + user.getId() + "] already exists");
        } catch (Exception e) {
            throw new IOException(e.toString(), e);
        }
    }

    @Override
    public List<User> findUsers(int offset, int limit) throws IOException {
        List<User> ans = new ArrayList<User>();
        List<ChildData> data = usersCache.getCurrentData();
        for (int i = offset, n = Math.min(offset + limit, data.size()); i < n; i++) {
            ChildData cd = data.get(i);
            User u = jsonService.fromJson(cd.getData(), User.class);
            u.setId(getLastPart(cd.getPath()));
            ans.add(u);
        }
        return ans;
    }

    @Override
    public int countUsers() throws IOException {
        return usersCache.getCurrentData().size();
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
