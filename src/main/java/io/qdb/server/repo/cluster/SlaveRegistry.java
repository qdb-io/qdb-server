package io.qdb.server.repo.cluster;

import io.qdb.server.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Keeps track of slaves for a server acting as master.
 */
@Singleton
public class SlaveRegistry {

    private static final Logger log = LoggerFactory.getLogger(SlaveRegistry.class);

    private final List<Slave> slaves = new ArrayList<Slave>();

    public SlaveRegistry() {
    }

    public synchronized Slave slaveConnected(String id, long txId) {
        Slave slave = find(id);
        boolean found = slave != null;
        if (found) {
            slave.disconnect(); // just in case it is still connected
        } else {
            slaves.add(slave = new Slave(id));
        }
        slave.thread = Thread.currentThread();
        slave.active(txId);
        if (log.isInfoEnabled()) log.info("Slave " + slave + (found ? " re-" : " ") + "connected txId " + txId);
        return slave;
    }

    private Slave find(String id) {
        for (Slave s : slaves) if (id.equals(s.id)) return s;
        return null;
    }

    /**
     * Disconnect all slaves and clear the list.
     */
    public void disconnectAndClear() {
        Slave[] copy;
        synchronized (this) {
            int n = slaves.size();
            if (n == 0) return;
            slaves.toArray(copy = new Slave[n]);
            slaves.clear();
        }
        for (Slave slave : copy) slave.disconnect();
    }

    public synchronized Repository.ServerStatus[] getSlaveStatuses() {
        int n = slaves.size();
        if (n == 0) return null;
        Repository.ServerStatus[] ans = new Repository.ServerStatus[n];
        long now = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            Slave slave = slaves.get(i);
            Repository.ServerStatus status = new Repository.ServerStatus(slave.id, Repository.ServerRole.SLAVE,
                    (int) (now - slave.lastContact), slave.errorMessage);
            status.connected = slave.isConnected();
            ans[i] = status;
        }
        Arrays.sort(ans);
        return ans;
    }

    public class Slave {

        public final String id;
        public Thread thread;

        public long lastContact;
        public long txId;
        public String errorMessage;

        public Slave(String id) {
            this.id = id;
        }

        public synchronized void active(long txId) {
            this.txId = txId;
            errorMessage = null;
            lastContact = System.currentTimeMillis();
        }

        public synchronized void setErrorMessage(String msg) {
            errorMessage = msg;
            lastContact = System.currentTimeMillis();
            log.warn("Error sending transactions to slave " + this + ": " + msg);
        }

        public synchronized void disconnected() {
            // an "old" request thread might try to disconnect after a new one has connected so test the thread
            if (thread == Thread.currentThread()) {
                if (log.isInfoEnabled()) log.info("Slave " + this + " disconnected");
                thread = null;
            }
        }

        public synchronized boolean isConnected() {
            return thread != null;
        }

        public synchronized void disconnect() {
            if (thread != null) {
                thread.interrupt();
                thread = null;
            }
        }

        @Override
        public String toString() {
            return id + (thread == null ? " Not connected" : " " + thread.getName());
        }
    }

}
