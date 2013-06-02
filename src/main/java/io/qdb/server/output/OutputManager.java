package io.qdb.server.output;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.qdb.server.model.Output;
import io.qdb.server.model.Repository;
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
    private final OutputHandlerFactory handlerFactory;
    private final QueueManager queueManager;
    private final Map<String, OutputJob> jobs = new ConcurrentHashMap<String, OutputJob>(); // queue id -> job
    private final ExecutorService threadPool;

    @Inject
    public OutputManager(EventBus eventBus, Repository repo, OutputHandlerFactory handlerFactory,
                QueueManager queueManager) {
        this.repo = repo;
        this.handlerFactory = handlerFactory;
        this.queueManager = queueManager;
        this.threadPool = new ThreadPoolExecutor(1, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                new ThreadFactoryBuilder().setNameFormat("output-manager-%d").setUncaughtExceptionHandler(this).build());
        eventBus.register(this);
        syncOutputs();
    }

    @Override
    public void close() throws IOException {
        threadPool.shutdownNow();
    }

    @Subscribe
    public void handleRepoEvent(Repository.ObjectEvent ev) {
        if (ev.value instanceof Output && ev.type != Repository.ObjectEvent.Type.DELETED) syncOutput((Output)ev.value);
        // todo handle deleted outputs
    }

    private void syncOutputs() {
        try {
            for (Output output : repo.findOutputs(0, -1)) syncOutput(output);
        } catch (IOException e) {
            log.error("Error syncing outputs: " + e, e);
        }
    }

    private synchronized void syncOutput(Output o) {
//        MessageBuffer mb = handlers.get(o.getId());
//        boolean newBuffer = mb == null;
//        if (newBuffer) {
//            File dir = queueStorageManager.findDir(o);
//            try {
//                mb = new PersistentMessageBuffer(dir);
//            } catch (IOException e) {
//                log.error("Error creating buffer for queue " + o + ": " + e, e);
//                return;
//            }
//            if (log.isDebugEnabled()) log.debug("Opened " + mb);
//        } else if (log.isDebugEnabled()) {
//            log.debug("Updating " + mb);
//        }
//        mb.setExecutor(threadPool);
//        updateBufferProperties(mb, o);
//        if (newBuffer) handlers.put(o.getId(), mb);
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        log.error(e.toString(), e);
    }
}
