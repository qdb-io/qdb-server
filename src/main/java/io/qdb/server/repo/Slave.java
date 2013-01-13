package io.qdb.server.repo;

/**
 * A slave connected (at some point) to us (the master).
 */
public class Slave {

    public final String id;

    public Thread thread;
    public long lastContact = System.currentTimeMillis();
    public long txId;
    public String errorMessage;

    public Slave(String id, long txId) {
        this.id = id;
        this.txId = txId;
        thread = Thread.currentThread();
    }

    public void active(long txId) {
        this.txId = txId;
        errorMessage = null;
        lastContact = System.currentTimeMillis();
        thread = Thread.currentThread();
    }

    public void setErrorMessage(String msg) {
        errorMessage = msg;
        lastContact = System.currentTimeMillis();
        thread = Thread.currentThread();
    }

    public void disconnected() {
        if (thread == Thread.currentThread()) thread = null;
    }

    public boolean isConnected() {
        return thread != null;
    }

    @Override
    public String toString() {
        return id + " " + thread;
    }
}
