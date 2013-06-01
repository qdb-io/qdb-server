package io.qdb.server.queue;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.qdb.buffer.MessageBuffer;
import io.qdb.buffer.PersistentMessageBuffer;
import io.qdb.server.model.Queue;
import io.qdb.server.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Keeps our local queues in sync with the repository by responding to events and periodically syncing all queues.
 */
@Singleton
public class QueueManager implements Closeable, Thread.UncaughtExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(QueueManager.class);

    private final Repository repo;
    private final QueueStorageManager queueStorageManager;
    private final Map<String, MessageBuffer> buffers = new ConcurrentHashMap<String, MessageBuffer>();
    private final ExecutorService threadPool;

    @Inject
    public QueueManager(EventBus eventBus, Repository repo, QueueStorageManager queueStorageManager) {
        this.repo = repo;
        this.queueStorageManager = queueStorageManager;
        this.threadPool = new ThreadPoolExecutor(2, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                new ThreadFactoryBuilder().setNameFormat("queue-manager-%d").setUncaughtExceptionHandler(this).build());
        eventBus.register(this);
        syncQueues();
    }

    @Override
    public void close() throws IOException {
        threadPool.shutdown();
        for (Map.Entry<String, MessageBuffer> e : buffers.entrySet()) {
            MessageBuffer mb = e.getValue();
            try {
                mb.close();
            } catch (IOException x) {
                log.error("Error closing " + mb);
            }
        }
    }

    @Subscribe
    public void handleQueueEvent(Repository.ObjectEvent ev) {
        if (ev.value instanceof Queue && ev.type != Repository.ObjectEvent.Type.DELETED) syncQueue((Queue)ev.value);
        // todo handle deleted queues
    }

    private void syncQueues() {
        try {
            for (Queue queue : repo.findQueues(0, -1)) syncQueue(queue);
        } catch (IOException e) {
            log.error("Error syncing queues: " + e, e);
        }
    }

    private synchronized void syncQueue(Queue q) {
        MessageBuffer mb = buffers.get(q.getId());
        boolean newBuffer = mb == null;
        if (newBuffer) {
            File dir = queueStorageManager.findDir(q);
            try {
                mb = new PersistentMessageBuffer(dir);
            } catch (IOException e) {
                log.error("Error creating buffer for queue " + q + ": " + e, e);
                return;
            }
            if (log.isDebugEnabled()) log.debug("Opened " + mb);
        } else if (log.isDebugEnabled()) {
            log.debug("Updating " + mb);
        }
        mb.setExecutor(threadPool);
        updateBufferProperties(mb, q);
        if (newBuffer) buffers.put(q.getId(), mb);
    }

    private void updateBufferProperties(MessageBuffer mb, Queue q) {
        try {
            mb.setMaxPayloadSize(q.getMaxPayloadSize());
        } catch (IllegalArgumentException e) {
            log.error("Error updating maxPayloadSize on " + mb + " for queue " + q, e);
        }
        try {
            mb.setMaxSize(q.getMaxSize());
        } catch (Exception e) {
            log.error("Error updating maxSize on " + mb + " for queue " + q, e);
        }
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        log.error(e.toString(), e);
    }

    /**
     * Get the buffer for q or null if it does not exist.
     */
    public MessageBuffer getBuffer(Queue q) {
        return buffers.get(q.getId());
    }
}
