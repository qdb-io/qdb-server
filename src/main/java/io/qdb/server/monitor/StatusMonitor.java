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

package io.qdb.server.monitor;

import io.qdb.server.model.ModelObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * Base class for a status monitor.
 */
public abstract class StatusMonitor<T extends ModelObject> implements Closeable {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final int warningRepeatSecs;

    private final Timer timer;
    private final Map<String, Status> statuses = new HashMap<String, Status>();

    protected static final Status OK = new Status(Status.Type.OK, null);

    public StatusMonitor(String name, int startDelay, int interval, int warningRepeat) throws IOException {
        this.warningRepeatSecs = warningRepeat;
        timer = new Timer(name + "-status-monitor", true);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                checkObjects();
            }
        }, startDelay * 1000L, interval * 1000L);
    }

    @Override
    public void close() {
        timer.cancel();
    }

    /**
     * Get the status of the object or null if no status is available.
     */
    public abstract Status getStatus(T o) throws IOException;

    /**
     * Get the objects to monitor.
     */
    protected abstract Collection<T> getObjects() throws IOException;

    /**
     * Return a path for o for error messages and so on.
     */
    protected abstract String toPath(T o) throws IOException;

    private void checkObjects() {
        try {
            Collection<T> objects = getObjects();
            for (T o : objects) {
                Status status = getStatus(o);
                if (status == null) continue;
                String id = o.getId();
                Status last = statuses.get(id);
                if (last == null) last = OK;
                if (status.type != Status.Type.OK) {
                    if (status.type.compareTo(last.type) > 0
                            || (status.created - last.created) / 1000 >= warningRepeatSecs) {
                        String msg = toPath(o) + ": " + status.message;
                        if (status.type == Status.Type.WARN) log.warn(msg);
                        else log.error(msg);
                        statuses.put(id, status);
                    }
                } else if (last.type != Status.Type.OK) {
                    statuses.put(id, status);
                    log.info(toPath(o) + " has recovered");
                }
            }
        } catch (IOException e) {
            log.error("Error checking status: " + e, e);
        }
    }
}
