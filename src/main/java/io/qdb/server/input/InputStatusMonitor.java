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

package io.qdb.server.input;

import io.qdb.server.databind.DurationParser;
import io.qdb.server.model.Database;
import io.qdb.server.model.Input;
import io.qdb.server.model.Queue;
import io.qdb.server.monitor.Status;
import io.qdb.server.monitor.StatusMonitor;
import io.qdb.server.repo.Repository;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Collection;

/**
 * Monitors the status of all inputs and logs warnings and errors as needed.
 */
@Singleton
public class InputStatusMonitor extends StatusMonitor<Input> {

    private final Repository repo;

    private final long started = System.currentTimeMillis();

    @Inject
    public InputStatusMonitor(Repository repo,
                              @Named("inputStatusMonitorStartDelay") int startDelay,
                              @Named("inputStatusMonitorInterval") int interval,
                              @Named("inputWarningRepeatSecs") int warningRepeat) throws IOException {
        super("input", startDelay, interval, warningRepeat);
        this.repo = repo;
    }

    @Override
    protected Collection<Input> getObjects() throws IOException {
        return repo.findInputs(0, -1);
    }

    /**
     * Get the status of the input or null if no status is available (not enabled).
     */
    public Status getStatus(Input in) {
        if (!in.isEnabled()) return null;

        int warnAfter = in.getWarnAfter();
        int errorAfter = in.getErrorAfter();
        if (warnAfter <= 0 && errorAfter <= 0) return OK;

        long newest = in.getLastMessageTimestamp();
        long ms = System.currentTimeMillis() - (newest <= 0L ? started : newest);
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

    protected String toPath(Input in) {
        try {
            StringBuilder b = new StringBuilder();
            Queue q = repo.findQueue(in.getQueue());
            Database db = repo.findDatabase(q.getDatabase());
            if (!"default".equals(db.getId())) b.append("/db/").append(db);
            b.append("/q/").append(db.getQueueForQid(q.getId()));
            b.append("/in/").append(q.getInputForInputId(in.getId()));
            return b.toString();
        } catch (IOException e) {
            log.error(e.toString(), e);
            return "input id " + in.getId();
        }
    }
}
