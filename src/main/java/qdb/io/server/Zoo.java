package qdb.io.server;

import org.apache.zookeeper.ZooKeeper;

/**
 * Holds our connection to ZooKeeper.
 */
public class Zoo {

    private ZooKeeper zooKeeper;

    public synchronized ZooKeeper get() {
        return zooKeeper;
    }

    public synchronized void clear() {
        zooKeeper = null;
    }
}
