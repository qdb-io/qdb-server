package qdb.io.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages our databases.
 */
@Singleton
public class DatabaseRegistry implements Closeable {

    private final MetaDataStore mds;
    private final Map<String, Database> databases = new HashMap<String, Database>();

    private static final Logger log = LoggerFactory.getLogger(DatabaseRegistry.class);

    @Inject
    public DatabaseRegistry(MetaDataStore mds) throws IOException {
        this.mds = mds;
        for (String name : mds.list("databases")) {
            String dbPath = "databases/" + name;
            try {
                Database db = mds.get(dbPath, Database.class);
                db.init(mds, dbPath);
                databases.put(name, db);
                if (log.isDebugEnabled()) log.debug("Opened " + dbPath + ", " + db.getQueueCount() + " queue(s)");
            } catch (IOException e) {
                log.error("Error opening database [" + dbPath + "]: " + e, e);
            }
        }
        log.info("Opened " + databases.size() + " database(s)");
    }

    @Override
    public synchronized void close() throws IOException {
        for (Map.Entry<String, Database> e : databases.entrySet()) {
            try {
                e.getValue().close();
            } catch (IOException e1) {
                log.error("Error closing database [" + e.getKey() + "]: " + e, e);
            }
        }
    }

    public synchronized int getDatabaseCount() {
        return databases.size();
    }

    public synchronized Database getDatabase(String name) {
        return databases.get(name);
    }
}
