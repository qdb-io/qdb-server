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

package io.qdb.server.output;

import io.qdb.buffer.MessageBuffer;
import io.qdb.server.model.Database;
import io.qdb.server.model.Output;
import io.qdb.server.model.Queue;
import io.qdb.server.monitor.Status;
import io.qdb.server.monitor.StatusMonitor;
import io.qdb.server.queue.QueueManager;
import io.qdb.server.repo.Repository;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Collection;

/**
 * Monitors the status of all outputs and logs warnings and errors as needed.
 */
@Singleton
public class OutputStatusMonitor extends StatusMonitor<Output> {

    private final Repository repo;
    private final QueueManager outputManager;

    @Inject
    public OutputStatusMonitor(Repository repo, QueueManager outputManager,
                               @Named("outputStatusMonitorStartDelay") int startDelay,
                               @Named("outputStatusMonitorInterval") int interval,
                               @Named("outputWarningRepeatSecs") int warningRepeat) throws IOException {
        super("output", startDelay, interval, warningRepeat);
        this.repo = repo;
        this.outputManager = outputManager;
    }

    @Override
    protected Collection<Output> getObjects() throws IOException {
        return repo.findOutputs(0, -1);
    }

    /**
     * Get the status of the output or null if no status is available (not enabled).
     */
    public Status getStatus(Output o) throws IOException {
        if (!o.isEnabled()) return null;

        Queue q = repo.findQueue(o.getQueue());
        if (q == null) return null;
        MessageBuffer mb = outputManager.getBuffer(q);
        if (mb == null) return null;

        double warnAfter = o.getWarnAfter();
        double errorAfter = o.getErrorAfter();
        if (warnAfter <= 0.0 && errorAfter <= 0.0) return OK;

        long behindByBytes = mb.getNextId() - o.getAtId();
        double p = behindByBytes * 100.0 / mb.getMaxSize();

        if (errorAfter > 0.0 && p >= errorAfter) {
            return new Status(Status.Type.ERROR, buildMessage(p));
        } else if (warnAfter > 0.0 && p >= warnAfter) {
            return new Status(Status.Type.WARN, buildMessage(p));
        }
        return OK;
    }

    private String buildMessage(double p) {
        return String.format("%.1f%% of queue used", p);
    }

    protected String toPath(Output o) {
        try {
            StringBuilder b = new StringBuilder();
            Queue q = repo.findQueue(o.getQueue());
            Database db = repo.findDatabase(q.getDatabase());
            if (!"default".equals(db.getId())) b.append("/db/").append(db);
            b.append("/q/").append(db.getQueueForQid(q.getId()));
            b.append("/out/").append(q.getOutputForOid(o.getId()));
            return b.toString();
        } catch (IOException e) {
            log.error(e.toString(), e);
            return "output id " + o.getId();
        }
    }
}
