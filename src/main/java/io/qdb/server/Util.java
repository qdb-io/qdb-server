package io.qdb.server;

import java.io.File;
import java.io.IOException;

/**
 * Static utility methods.
 */
public class Util {

    /**
     * Ensure that dir exists, creating it if needed and that it is a writeable directory.
     */
    public static File ensureDirectory(File dir) throws IOException {
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException("Unable to create directory [" + dir.getAbsolutePath() + "]");
            }
        } else if (!dir.isDirectory()) {
            throw new IOException("Not a directory [" + dir.getAbsolutePath() + "]");
        } else if (!dir.canWrite()) {
            throw new IOException("Not writable [" + dir.getAbsolutePath() + "]");
        }
        return dir.getAbsoluteFile();
    }

}
