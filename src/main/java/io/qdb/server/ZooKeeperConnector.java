package io.qdb.server;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
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
public class ZooKeeperConnector implements Watcher {

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperConnector.class);

    private final Zoo zoo;
    private final ServerInfo serverInfo;
    private final QueueManager queueManager;
    private final String connectString;
    private final int sessionTimeout;

    @Inject
    public ZooKeeperConnector(Zoo zoo, ServerInfo serverInfo, QueueManager queueManager,
                @Named("zookeeper.connectString") String connectString,
                @Named("zookeeper.sessionTimeout") int sessionTimeout) {
        this.zoo = zoo;
        this.serverInfo = serverInfo;
        this.queueManager = queueManager;
        this.connectString = connectString;
        this.sessionTimeout = sessionTimeout;
    }

    public synchronized void ensureConnected() throws IOException {
        if (zoo.getZooKeeper() == null) {
            log.info("Connecting to ZooKeeper(s) [" + connectString + "] sessionTimeout " + sessionTimeout);
            zoo.setZooKeeper(new ZooKeeper(connectString, sessionTimeout, this));
        }
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            log.debug(event.toString());
            Event.KeeperState state = event.getState();
            if (state == Event.KeeperState.SyncConnected) {
                zoo.createRootNode();
                serverInfo.publish();
                queueManager.syncQueues();
            }
        } catch (Exception e) {
            log.error(e.toString(), e);
        }
    }
}
