package qdb.io.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Collection of MessageStore's.
 */
public class Database {

    private MetaDataStore mds;
    private String path;
    private Map<String, MessageQueue> queues;

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
                log.error("Error opening message queue [" + queuePath + "]: " + e, e);
            }
        }
    }

    public int getQueueCount() {
        return queues.size();
    }

}
