package io.qdb.server;

import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.*;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.SecureRandom;

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
            id = Files.readFirstLine(f, Charset.forName("UTF8"));
            if (id == null || id.trim().length() == 0) {
                throw new IOException("Server ID from [" + f.getAbsolutePath() + "] is empty");
            }
            if (log.isDebugEnabled()) log.debug("Server ID is [" + id + "] from [" + f.getAbsolutePath() + "]");
        } else {
            byte[] data = new byte[8];
            new SecureRandom().nextBytes(data);
            id = new BigInteger(data).abs().toString(36);
            Util.ensureDirectory(f.getParentFile());
            Files.write(id.getBytes("UTF8"), f);
            log.info("New server ID [" + id + "] stored in [" + f.getAbsolutePath() + "]");
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
