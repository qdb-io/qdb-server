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
import io.qdb.server.monitor.Status;
import io.qdb.server.monitor.StatusMonitor;
import io.qdb.server.repo.Repository;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.*;

/**
 * Monitors the status of all queues and logs warnings and errors as needed.
 */
@Singleton
public class QueueStatusMonitor extends StatusMonitor<Queue> {

    private final Repository repo;
    private final QueueManager queueManager;

    @Inject
    public QueueStatusMonitor(Repository repo, QueueManager queueManager,
                @Named("queueStatusMonitorStartDelay") int startDelay,
                @Named("queueStatusMonitorInterval") int interval,
                @Named("queueWarningRepeatSecs") int warningRepeat) throws IOException {
        super("queue", startDelay, interval, warningRepeat);
        this.repo = repo;
        this.queueManager = queueManager;
    }

    @Override
    protected Collection<Queue> getObjects() throws IOException {
        return repo.findQueues(0, -1);
    }

    /**
     * Get the status of the queue or null if no status is available (it doesn't have a buffer yet).
     */
    public Status getStatus(Queue q) throws IOException {
        MessageBuffer mb = queueManager.getBuffer(q);
        if (mb == null) return null;

        int warnAfter = q.getWarnAfter();
        int errorAfter = q.getErrorAfter();
        if (warnAfter <= 0 && errorAfter <= 0) return OK;

        Date newest = mb.getMostRecentTimestamp();
        long ms = System.currentTimeMillis() - (newest == null ? mb.getCreationTime() : newest.getTime());
        int secs = (int)(ms / 1000);

        if (errorAfter > 0 && secs >= errorAfter) {
            return new Status(Status.Type.ERROR, buildMessage(ms));
        } else if (warnAfter > 0 && secs >= warnAfter) {
            return new Status(Status.Type.WARN, buildMessage(ms));
        }
        return OK;
    }

    private String buildMessage(long ms) {
        return "Last message appended " + DurationParser.formatHumanMs(ms) + " ago";
    }

    protected String toPath(Queue q) {
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
