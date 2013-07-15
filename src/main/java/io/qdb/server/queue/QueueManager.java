/*
 * Copyright 2013 David Tinker
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.qdb.server.queue;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.qdb.buffer.MessageBuffer;
import io.qdb.buffer.PersistentMessageBuffer;
import io.qdb.server.model.Queue;
import io.qdb.server.repo.Repository;
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

    private final QueueStorageManager queueStorageManager;
    private final Map<String, MessageBuffer> buffers = new ConcurrentHashMap<String, MessageBuffer>();
    private final ExecutorService threadPool;

    @Inject
    public QueueManager(EventBus eventBus, Repository repo, QueueStorageManager queueStorageManager) throws IOException {
        this.queueStorageManager = queueStorageManager;
        this.threadPool = new ThreadPoolExecutor(2, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                new ThreadFactoryBuilder().setNameFormat("queue-manager-%d").setUncaughtExceptionHandler(this).build());
        eventBus.register(this);
        for (Queue queue : repo.findQueues(0, -1)) syncQueue(queue);
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

    private synchronized void syncQueue(Queue q) {
        MessageBuffer mb = buffers.get(q.getId());
        boolean newBuffer = mb == null;
        if (newBuffer) {
            try {
                File dir = queueStorageManager.findDir(q);
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
