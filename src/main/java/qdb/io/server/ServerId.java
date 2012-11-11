package qdb.io.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.*;
import java.util.UUID;

/**
 * Manages our unique server ID. This is read from a file in the data.dir. The file is created if it does not
 * exist and populated with a UUID.
 */
public class ServerId {

    private static final Logger log = LoggerFactory.getLogger(ServerId.class);

    private final String id;

    @Inject
    public ServerId(Storage storage) throws IOException {
        File f = new File(storage.getDataDir(), "server-id.txt");
        if (f.exists()) {
            BufferedReader r = new BufferedReader(new FileReader(f));
            try {
                id = r.readLine();
            } finally {
                r.close();
            }
        } else {
            id = UUID.randomUUID().toString();
            FileWriter w = new FileWriter(f);
            try {
                w.write(id);
            } finally {
                w.close();
            }
            log.info("Generated new server ID: " + id);
        }
    }

    public String get() {
        return id;
    }

    @Override
    public String toString() {
        return id;
    }
}
