package qdb.io.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Collection of MessageStore's.
 */
public class Database implements Closeable {

    private MetaDataStore mds;
    private String path;
    private Map<String, MessageQueue> queues = new HashMap<String, MessageQueue>();

    private static final Logger log = LoggerFactory.getLogger(Database.class);

    public Database() {
    }

    public void init(MetaDataStore mds, String path) throws IOException {
        this.mds = mds;
        this.path = path;
        for (String name : mds.list(path)) {
            String queuePath = path + "/" + name;
            try {
                MessageQueue queue = mds.get(queuePath, MessageQueue.class);
                queue.init();
                queues.put(name, queue);
                if (log.isDebugEnabled()) log.debug("Opened " + queuePath);
            } catch (IOException e) {
                log.error("Error opening queue [" + queuePath + "]: " + e, e);
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        for (Map.Entry<String, MessageQueue> e : queues.entrySet()) {
            try {
                e.getValue().close();
            } catch (IOException e1) {
                log.error("Error closing queue [" + path + "/" + e.getKey() + "]: " + e, e);
            }
        }
    }

    public synchronized int getQueueCount() {
        return queues.size();
    }

    public synchronized MessageQueue getQueue(String name) {
        return queues.get(name);
    }

}
