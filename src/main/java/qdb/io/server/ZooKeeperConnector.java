package qdb.io.server;

import com.typesafe.config.Config;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        cfg = cfg.getConfig("zookeeper");
        this.connectString = cfg.getString("connectString");
        this.sessionTimeout = cfg.getInt("sessionTimeout");
    }

    public synchronized void ensureConnected() throws IOException {
        if (zoo.getZooKeeper() == null) {
            log.info("Connecting to ZooKeeper(s) [" + connectString + "] sessionTimeout " + sessionTimeout);
            zoo.setZooKeeper(new ZooKeeper(connectString, sessionTimeout, watcher));
        }
    }
}
