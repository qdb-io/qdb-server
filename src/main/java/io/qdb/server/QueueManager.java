package io.qdb.server;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Watches queue meta data in ZooKeeper and maintains a corresponding MessageQueue instance for each queue.
 */
@Singleton
public class QueueManager implements Watcher {

    private static final Logger log = LoggerFactory.getLogger(QueueManager.class);

    private final LocalStorage storage;
    private final Zoo zoo;
    private final File queueDir;

    @Inject
    public QueueManager(LocalStorage storage, Zoo zoo) throws IOException {
        this.storage = storage;
        this.zoo = zoo;
        storage.ensureDirExists("queues", queueDir = new File(storage.getDataDir(), "queues"));
    }

    public void syncQueues() throws InterruptedException, KeeperException {
        zoo.ensure("/queues", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        List<String> list = zoo.getChildren("/queues", this);
        Collections.sort(list);
    }

    @Override
    public void process(WatchedEvent event) {
        log.debug(event.toString());
    }
}
