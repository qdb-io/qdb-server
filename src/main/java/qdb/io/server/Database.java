package qdb.io.server;

import java.io.IOException;
import java.util.Map;

/**
 * Collection of MessageStore's.
 */
public class Database {

    private String name;
    private Map<String, MessageStore> stores;

    public Database(MetaDataStore mds, String path, String name) throws IOException {

    }

}
