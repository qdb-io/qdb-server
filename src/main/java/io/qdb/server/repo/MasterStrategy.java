package io.qdb.server.repo;

import io.qdb.server.model.Server;

import java.io.Closeable;

/**
 * Responsible for choosing the master for the cluster and posting an event to the shared event bus when one has
 * been selected.
 */
public interface MasterStrategy extends Closeable {

    /**
     * Start choosing the master server for the cluster. Posts a {@link MasterFound} event when done. Note that
     * assuming the master is known or can be discovered an event will always be posted in response to this even
     * if the master hasn't changed. So clients wanting to know who the master is should call this method and
     * look for the event (which might never arrive if we cannot find a master).
     */
    void chooseMaster();

    /**
     * Return a short human readable string indicating the current status (e.g. "Electing master").
     */
    String getStatus();

    /**
     * This event is posted when a master has been selected.
     */
    public static class MasterFound {

        public final Server master;

        public MasterFound(Server master) {
            this.master = master;
        }

        @Override
        public String toString() {
            return "MasterFound " + master;
        }
    }
}
