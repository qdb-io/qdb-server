package qdb.io.server;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Listens for events from ZooKeeper and dispatches them for processing.
 */
@Singleton
public class ZooWatcher implements Watcher {

    private static final Logger log = LoggerFactory.getLogger(ZooWatcher.class);

    private final ServerInfo serverInfo;

    @Inject
    public ZooWatcher(ServerInfo serverInfo) {
        this.serverInfo = serverInfo;
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            log.debug(event.toString());
            Event.KeeperState state = event.getState();
            if (state == Event.KeeperState.SyncConnected) {
                serverInfo.publish();
            }
        } catch (Exception e) {
            log.error(e.toString(), e);
        }
    }
}
