package qdb.io.server;

import com.typesafe.config.Config;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import javax.inject.Inject;
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
    public Zoo(Config cfg) {
        rootPath = "/qdb/" + cfg.getString("cluster.name");
    }

    public String create(String path, byte data[], List<ACL> acl, CreateMode createMode)
            throws InterruptedException, KeeperException {
        return getZooKeeper().create(rootPath + path, data, acl, createMode);
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
