package qdb.io.server;

import org.apache.zookeeper.ZooKeeper;

import javax.inject.Singleton;
import java.io.Closeable;
import java.io.IOException;

/**
 * Holds our connection to ZooKeeper.
 */
@Singleton
public class Zoo implements Closeable {

    private ZooKeeper zooKeeper;

    public synchronized ZooKeeper get() {
        return zooKeeper;
    }

    public synchronized void set(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
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
