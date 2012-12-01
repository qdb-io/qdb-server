package io.qdb.server.zoo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Holds our connection to ZooKeeper and ensures that our root node already exists. This is /qdb/[clusterName].
 */
@Singleton
public class Zoo implements Closeable {

    private final String rootPath;

    private ZooKeeper zooKeeper;

    @Inject
    public Zoo(@Named("clusterName") String clusterName) {
        rootPath = "/qdb/" + clusterName;
    }

    /**
     * ZooKeeper nodes should only be created under this path.
     */
    public String getRootPath() {
        return rootPath;
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
