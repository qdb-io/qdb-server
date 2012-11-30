package io.qdb.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;

/**
 * Manages where we put data on the local filesystem.
 */
@Singleton
public class LocalStorage {

    private static final Logger log = LoggerFactory.getLogger(LocalStorage.class);

    private File dataDir;

    @Inject
    public LocalStorage(@Named("data.dir") String dir) throws IOException {
        ensureDirExists("data.dir", dataDir = new File(dir));
        log.info("data.dir = [" + dataDir.getAbsolutePath() + "]");
    }

    public File getDataDir() {
        return dataDir;
    }

    /**
     * Ensure that a directory exists. The description parameter is used to generate error messages.
     */
    public void ensureDirExists(String description, File dir) throws IOException {
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException("Unable to create directory " + description + " [" + dir + "]");
            }
        }
        if (!dir.isDirectory()) {
            throw new IOException("Not a directory " + description + " [" + dir + "]");
        }
        if (!dir.canWrite()) {
            throw new IOException("Not writable " + description + " [" + dir + "]");
        }
    }
}
