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

import io.qdb.buffer.MessageBuffer;
import io.qdb.server.databind.DurationParser;
import io.qdb.server.model.Database;
import io.qdb.server.model.Queue;
import io.qdb.server.repo.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Monitors the status of all queues and logs warnings and errors as needed.
 */
@Singleton
public class QueueStatusMonitor implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(QueueStatusMonitor.class);

    private final Repository repo;
    private final QueueManager queueManager;
    private final int queueWarningRepeatSecs;

    private final Timer timer = new Timer("queue-status-monitor", true);
    private final Map<String, QueueStatus> statuses = new HashMap<String, QueueStatus>();

    private static final QueueStatus OK = new QueueStatus(QueueStatus.Type.OK, null);

    @Inject
    public QueueStatusMonitor(Repository repo, QueueManager queueManager,
                @Named("queueStatusMonitorStartDelay") int queueStatusMonitorStartDelay,
                @Named("queueStatusMonitorInterval") int queueStatusMonitorInterval,
                @Named("queueWarningRepeatSecs") int queueWarningRepeat) throws IOException {
        this.repo = repo;
        this.queueManager = queueManager;
        this.queueWarningRepeatSecs = queueWarningRepeat;
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                checkQueues();
            }
        }, queueStatusMonitorStartDelay * 1000L, queueStatusMonitorInterval * 1000L);
    }

    @Override
    public void close() {
        timer.cancel();
    }

    /**
     * Get the status of the queue or null if no status is available (it doesn't have a buffer yet).
     */
    public QueueStatus getStatus(Queue q) throws IOException {
        MessageBuffer mb = queueManager.getBuffer(q);
        if (mb == null) return null;

        int warnAfter = q.getWarnAfter();
        int errorAfter = q.getErrorAfter();
        if (warnAfter <= 0 && errorAfter <= 0) return OK;

        Date newest = mb.getMostRecentTimestamp();
        long ms = System.currentTimeMillis() - (newest == null ? mb.getCreationTime() : newest.getTime());
        int secs = (int)(ms / 1000);

        if (errorAfter > 0 && secs >= errorAfter) {
            return new QueueStatus(QueueStatus.Type.ERROR, buildMessage(ms));
        } else if (secs >= warnAfter) {
            return new QueueStatus(QueueStatus.Type.WARN, buildMessage(ms));
        }
        return OK;
    }

    private String buildMessage(long ms) {
        return "Last message appended " + DurationParser.formatHumanMs(ms) + " ago";
    }

    private void checkQueues() {
        try {
            List<Queue> queues = repo.findQueues(0, -1);
            for (Queue q : queues) {
                QueueStatus status = getStatus(q);
                if (status == null) continue;
                String qid = q.getId();
                QueueStatus last = statuses.get(qid);
                if (last == null) last = OK;
                if (status.type != QueueStatus.Type.OK) {
                    if (status.type.compareTo(last.type) > 0
                            || (status.created - last.created) / 1000 >= queueWarningRepeatSecs) {
                        String msg = toPath(q) + ": " + status.message;
                        if (status.type == QueueStatus.Type.WARN) log.warn(msg);
                        else log.error(msg);
                        statuses.put(qid, status);
                    }
                } else if (last.type != QueueStatus.Type.OK) {
                    statuses.put(qid, status);
                    log.info(toPath(q) + " has recovered");
                }
            }
        } catch (IOException e) {
            log.error("Error checking status of queues: " + e, e);
        }
    }

    private String toPath(Queue q) {
        try {
            StringBuilder b = new StringBuilder();
            Database db = repo.findDatabase(q.getDatabase());
            if (!"default".equals(db.getId())) b.append("/db/").append(db);
            b.append("/q/").append(db.getQueueForQid(q.getId()));
            return b.toString();
        } catch (IOException e) {
            log.error(e.toString(), e);
            return "queue id " + q.getId();
        }
    }
}
