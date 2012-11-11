package qdb.io.server;

import com.typesafe.config.Config;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.Closeable;
import java.io.IOException;

/**
 * Manages our connection to ZooKeeper.
 */
@Singleton
public class ZooKeeperConnector implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperConnector.class);

    private final String connectString;
    private final int sessionTimeout;
    private final ZooWatcher watcher;

    private ZooKeeper zooKeeper;

    @Inject
    public ZooKeeperConnector(Config cfg, ZooWatcher watcher) throws IOException {
        this.watcher = watcher;

        String clusterName = cfg.getString("cluster.name");
        cfg = cfg.getConfig("zookeeper");

        // add the cluster name to the 'chroot' part of each connect string
        String[] zkNodes = cfg.getString("connectString").split("[/w]*,[/w]*");
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < zkNodes.length; i++) {
            if (i > 0) b.append(',');
            String node = zkNodes[i];
            b.append(node);
            if (node.indexOf('/') < 0) b.append("/qdb");
            b.append('/').append(clusterName);
        }
        this.connectString = b.toString();

        this.sessionTimeout = cfg.getInt("sessionTimeout");
    }

    @PostConstruct
    private synchronized void connect() throws IOException {
        log.info("Connecting to ZooKeeper(s) [" + connectString + "] sessionTimeout " + sessionTimeout);
        zooKeeper = new ZooKeeper(connectString, sessionTimeout, watcher);
    }

    /**
     * Get our ZooKeeper instance, attempting to connect if we are not already connected.
     */
    public synchronized ZooKeeper get() throws IOException {
        if (zooKeeper == null) connect();
        return zooKeeper;
    }

    @Override
    public synchronized void close() throws IOException {
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
                zooKeeper = null;
            } catch (InterruptedException e) {
                throw new IOException("Error closing ZooKeeper connection: " + e, e);
            }
        }
    }
}
