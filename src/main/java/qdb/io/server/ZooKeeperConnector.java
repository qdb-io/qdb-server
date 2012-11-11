package qdb.io.server;

import com.typesafe.config.Config;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

/**
 * Manages our connection to ZooKeeper.
 */
@Singleton
public class ZooKeeperConnector {

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperConnector.class);

    private final ZooWatcher watcher;
    private final Zoo zoo;

    private final String connectString;
    private final int sessionTimeout;

    @Inject
    public ZooKeeperConnector(Config cfg, ZooWatcher watcher, Zoo zoo) throws IOException {
        this.watcher = watcher;
        this.zoo = zoo;

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

    public synchronized void ensureConnected() throws IOException {
        if (zoo.get() == null) {
            log.info("Connecting to ZooKeeper(s) [" + connectString + "] sessionTimeout " + sessionTimeout);
            zoo.set(new ZooKeeper(connectString, sessionTimeout, watcher));
        }
    }
}
