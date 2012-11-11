package qdb.io.server.zookeeper;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;

/**
 * Manages our connection to ZooKeeper.
 */
@Singleton
public class ZooKeeperConnector {

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperConnector.class);

    @Inject
    private ZooWatcher watcher;
    @Inject
    private Zoo zoo;
    @Inject @Named("zookeeper.connectString")
    private String connectString;
    @Inject @Named("zookeeper.sessionTimeout")
    private int sessionTimeout;

    public synchronized void ensureConnected() throws IOException {
        if (zoo.getZooKeeper() == null) {
            log.info("Connecting to ZooKeeper(s) [" + connectString + "] sessionTimeout " + sessionTimeout);
            zoo.setZooKeeper(new ZooKeeper(connectString, sessionTimeout, watcher));
        }
    }
}
