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
import io.qdb.server.model.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Monitors the status of all queues and logs warnings and errors as needed.
 */
@Singleton
public class QueueStatusMonitor {

    private static final Logger log = LoggerFactory.getLogger(QueueStatusMonitor.class);

    private final QueueManager queueManager;

    private final Map<String, QueueStatus> statuses = new ConcurrentHashMap<String, QueueStatus>();

    private static final QueueStatus OK = new QueueStatus(QueueStatus.Type.OK, null);

    @Inject
    public QueueStatusMonitor(QueueManager queueManager) throws IOException {
        this.queueManager = queueManager;
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

}
