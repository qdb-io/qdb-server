package io.qdb.server.util;

/**
 * Runnable that can be stopped.
 */
public abstract class StoppableTask implements Runnable {

    private Thread thread;
    private boolean stopped;

    @Override
    public void run() {
        synchronized (this) {
            thread = Thread.currentThread();
        }
        try {
            runImpl();
        } finally {
            synchronized (this) {
                thread = null;
            }
        }
    }

    public void stop() {
        synchronized (this) {
            stopped = true;
            if (thread != null) thread.interrupt();
        }
    }

    public synchronized boolean isStopped() {
        return stopped;
    }

    protected abstract void runImpl();

}
