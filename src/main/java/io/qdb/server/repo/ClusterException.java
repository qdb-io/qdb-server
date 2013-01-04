package io.qdb.server.repo;

import java.io.IOException;

/**
 * Something has gone wrong with something to do with clustering.
 */
public class ClusterException extends IOException {

    public static class NotMaster extends ClusterException {
        public NotMaster() {
            super("Not the master of the cluster");
        }
    }

    public ClusterException(String message) {
        super(message);
    }

    public ClusterException(String message, Throwable cause) {
        super(message, cause);
    }
}
