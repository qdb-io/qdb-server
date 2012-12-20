package io.qdb.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.*;
import java.util.UUID;

/**
 * Reads/generates the unique ID for a qdb server from its data dir.
 */
@Singleton
public class ServerId {

    private static final Logger log = LoggerFactory.getLogger(ServerId.class);

    private final String id;

    @Inject
    public ServerId(@Named("data.dir") String dataDir) throws IOException {
        File f = new File(dataDir, "server-id.txt");
        if (f.exists()) {
            BufferedReader r = new BufferedReader(new FileReader(f));
            try {
                id = r.readLine();
            } finally {
                r.close();
            }
            if (id == null || id.trim().length() == 0) {
                throw new IOException("Server ID from [" + f.getAbsolutePath() + "] is empty");
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
