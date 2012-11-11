package qdb.io.server;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;

/**
 * Manages where we put data on the local filesystem.
 */
@Singleton
public class Storage {

    private static final Logger log = LoggerFactory.getLogger(Storage.class);

    private File dataDir;

    @Inject
    public Storage(Config cfg) throws IOException {
        dataDir = new File(cfg.getString("data.dir"));
        if (!dataDir.exists()) {
            if (!dataDir.mkdirs()) {
                throw new IOException("Unable to create data.dir [" + dataDir + "]");
            }
        }
        if (!dataDir.isDirectory()) {
            throw new IOException("data.dir [" + dataDir + "] is not a directory");
        }
        if (!dataDir.canWrite()) {
            throw new IOException("data.dir [" + dataDir + "] is not a writeable");
        }
        log.info("data.dir = [" + dataDir.getAbsolutePath() + "]");
    }

    public File getDataDir() {
        return dataDir;
    }
}
