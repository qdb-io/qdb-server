package qdb.io.server;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages our databases.
 */
@Singleton
public class DatabaseRegistry {

    private final MetaDataStore mds;
    private final Map<String, Database> databases = new HashMap<String, Database>();

    @Inject
    public DatabaseRegistry(MetaDataStore mds) throws IOException {
        this.mds = mds;
        for (String name : mds.list("databases")) {
            databases.put(name, new Database(mds, "databases", name));
        }
    }
}
