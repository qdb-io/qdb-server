package io.qdb.server.output;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.qdb.buffer.MessageBuffer;
import io.qdb.server.databind.DataBinder;
import io.qdb.server.model.Database;
import io.qdb.server.model.Output;
import io.qdb.server.model.Queue;
import io.qdb.server.repo.Repository;
import io.qdb.server.queue.QueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Ensures that each enabled output has a corresponding {@link OutputJob} instance running to process its queue.
 * Detects changes to outputs by listening to repository events.
 */
@Singleton
public class OutputManager implements Closeable, Thread.UncaughtExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(OutputManager.class);

    private final Repository repo;
    private final QueueManager queueManager;
    private final OutputHandlerFactory handlerFactory;
    private final Map<String, OutputJob> jobs = new ConcurrentHashMap<String, OutputJob>(); // output id -> job
    private final ExecutorService pool;

    @Inject
    public OutputManager(EventBus eventBus, Repository repo, QueueManager queueManager,
                         OutputHandlerFactory handlerFactory) throws IOException {
        this.repo = repo;
        this.queueManager = queueManager;
        this.handlerFactory = handlerFactory;
        this.pool = new ThreadPoolExecutor(1, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                new ThreadFactoryBuilder().setNameFormat("output-manager-%d").setUncaughtExceptionHandler(this).build());
        eventBus.register(this);
        for (Output output : this.repo.findOutputs(0, -1)) outputChanged(output);
    }

    @Override
    public void close() throws IOException {
        pool.shutdownNow();
    }

    @Subscribe
    public void handleRepoEvent(Repository.ObjectEvent ev) {
        if (ev.value instanceof Output) {
            Output output = (Output) ev.value;
            if (ev.type == Repository.ObjectEvent.Type.DELETED) {
                synchronized (this) {
                    OutputJob job = jobs.remove(output.getId());
                    if (job != null) job.stop();
                }
            } else {
                outputChanged(output);
            }
        }
    }

    private synchronized void outputChanged(Output o) {
        String oid = o.getId();

        OutputJob existing = jobs.get(oid);
        if (existing != null) {
            existing.outputChanged(o);  // this will cause job to exit if it didn't make the change
            return;
        }

        if (!o.isEnabled()) return;

        OutputHandler handler;
        try {
            handler = handlerFactory.createHandler(o.getType());
        } catch (IllegalArgumentException e) {
            log.error("Error creating handler for " + tos(o) + ": " + e.getMessage(), e);
            return;
        }

        final OutputJob job = new OutputJob(repo, oid, handler);
        jobs.put(oid, job);
        pool.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    execJob(job);
                } catch (Exception e) {
                    log.error(e.toString(), e);
                }
            }
        });
    }

    private void execJob(OutputJob job) throws Exception {
        final String oid = job.getOid();
        int reSyncDelayMs = 1000;
        try {
            Output output = repo.findOutput(oid);
            if (output == null) {
                if (log.isDebugEnabled()) log.debug("Output [" + oid + "] does not exist");
                reSyncDelayMs = -1;
                return;
            }
            Queue q = repo.findQueue(output.getQueue());
            if (q == null) {
                if (log.isDebugEnabled()) log.debug("Queue [" + output.getQueue() + "] does not exist");
                reSyncDelayMs = -1;
                return;
            }
            try {
                OutputHandler h = job.getHandler();
                Map<String, Object> p = output.getParams();
                if (p != null) new DataBinder().ignoreInvalidFields(true).bind(p, h).check();
                h.init(q, output);
            } catch (IllegalArgumentException e) {
                String msg = "Output " + tos(output) + ": " +
                        (e instanceof IllegalArgumentException ? e.getMessage() : e.toString());
                if (e instanceof IllegalArgumentException) log.error(msg);
                else log.error(msg, e);
                reSyncDelayMs = -1;
                return;
            }

            MessageBuffer buffer = queueManager.getBuffer(q);
            if (buffer == null) {   // we might be busy starting up or something
                if (log.isDebugEnabled()) log.debug("Queue [" + q.getId() + "] does not have a buffer");
                return;
            }
            job.processMessages(output, buffer);
            reSyncDelayMs = 0;
        } finally {
            jobs.remove(oid);
            if (reSyncDelayMs >= 0) syncOutputLater(reSyncDelayMs, oid);
        }
    }

    private void syncOutputLater(final int delayMs, final String oid) {
        pool.execute(new Runnable() {
            @Override
            public void run() {
                if (delayMs > 0) {
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException e) {
                        return; // someone trying to get us to stop
                    }
                }
                try {
                    Output output = repo.findOutput(oid);
                    if (output != null) outputChanged(output);
                } catch (Exception e) {
                    log.error(e.toString(), e);
                    syncOutputLater(1000, oid);
                }
            }
        });
    }

    private String tos(Output o) {
        try {
            Queue q = repo.findQueue(o.getQueue());
            if (q != null) {
                Database db = repo.findDatabase(q.getDatabase());
                if (db != null) {
                    StringBuilder b = new StringBuilder();
                    b.append("/databases/").append(db.getId());
                    String s = db.getQueueForQid(q.getId());
                    if (s != null) {
                        b.append("/queues/").append(s);
                        s = q.getOutputForOid(o.getId());
                        if (s != null) {
                            return b.append("/outputs/").append(s).append('(').append(o.getType()).append(')').toString();
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.error(e.toString(), e);
        }
        return o.toString();
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        log.error(e.toString(), e);
    }
}
