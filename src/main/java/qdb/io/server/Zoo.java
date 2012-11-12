package qdb.io.server;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Holds our connection to ZooKeeper. Proxies methods on ZooKeeper to prepend our root path to all paths.
 */
@Singleton
public class Zoo implements Closeable {

    private final String rootPath;

    private ZooKeeper zooKeeper;

    @Inject
    public Zoo(@Named("clusterName") String clusterName) {
        rootPath = "/qdb/" + clusterName;
    }

    public String create(String path, byte data[], List<ACL> acl, CreateMode createMode)
            throws InterruptedException, KeeperException {
        return getZooKeeper().create(rootPath + path, data, acl, createMode);
    }

    /**
     * Like {@link ZooKeeper#create(String, byte[], java.util.List, org.apache.zookeeper.CreateMode)} but
     * does not throw an exception if the node exists.
     */
    public void ensure(String path, byte data[], List<ACL> acl, CreateMode createMode)
            throws InterruptedException, KeeperException {
        try {
            create(path, data, acl, createMode);
        } catch (KeeperException.NodeExistsException ignore) {
        }
    }

    public List<String> getChildren(final String path, Watcher watcher) throws InterruptedException, KeeperException {
        return getZooKeeper().getChildren(rootPath + path, watcher);
    }

    public synchronized ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public synchronized void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    /**
     * Create the node for our root path.
     */
    public void createRootNode() throws InterruptedException, KeeperException {
        ZooKeeper zk = getZooKeeper();
        try {
            zk.create("/qdb", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ignore) {
        }
        try {
            zk.create(rootPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ignore) {
        }

    }

    @Override
    public synchronized void close() throws IOException {
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                throw new IOException("Error closing ZooKeeper connection: " + e, e);
            }
            zooKeeper = null;
        }
    }
}
