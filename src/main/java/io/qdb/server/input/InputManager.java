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

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.qdb.server.controller.JsonService;
import io.qdb.server.model.Input;
import io.qdb.server.queue.QueueManager;
import io.qdb.server.repo.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Ensures that each enabled input has a corresponding {@link InputJob} instance running to fetch messages.
 * Detects changes to inputs by listening to repository events.
 */
@Singleton
public class InputManager implements Closeable, Thread.UncaughtExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(InputManager.class);

    private final Repository repo;
    private final QueueManager queueManager;
    private final InputHandlerFactory handlerFactory;
    private final JsonService jsonService;
    private final Map<String, InputJob> jobs = new ConcurrentHashMap<String, InputJob>(); // input id -> job
    private final ExecutorService pool;

    @Inject
    public InputManager(EventBus eventBus, Repository repo, QueueManager queueManager,
                        InputHandlerFactory handlerFactory, JsonService jsonService) throws IOException {
        this.repo = repo;
        this.queueManager = queueManager;
        this.handlerFactory = handlerFactory;
        this.jsonService = jsonService;
        this.pool = new ThreadPoolExecutor(1, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                new ThreadFactoryBuilder().setNameFormat("input-%d").setUncaughtExceptionHandler(this).build());
        eventBus.register(this);
        for (Input input : this.repo.findInputs(0, -1)) inputChanged(input);
    }

    @Override
    public void close() throws IOException {
        List<InputJob> busy = new ArrayList<InputJob>(jobs.values());
        for (InputJob job : busy) job.stop();
        pool.shutdownNow();
    }

    @Subscribe
    public void handleRepoEvent(Repository.ObjectEvent ev) {
        if (ev.value instanceof Input) {
            Input input = (Input) ev.value;
            if (ev.type == Repository.ObjectEvent.Type.DELETED) {
                synchronized (this) {
                    InputJob job = jobs.remove(input.getId());
                    if (job != null) job.stop();
                }
            } else {
                inputChanged(input);
            }
        }
    }

    private synchronized void inputChanged(Input in) {
        String inputId = in.getId();

        InputJob existing = jobs.get(inputId);
        if (existing != null) {
            existing.inputChanged(in);  // this will cause job to restart if it didn't make the change
            return;
        }

        if (!in.isEnabled()) return;

        InputJob job = new InputJob(this, handlerFactory, queueManager, repo, jsonService, inputId);
        jobs.put(inputId, job);
        pool.execute(job);
    }

    void onInputJobExit(InputJob job) {
        jobs.remove(job.getInputId());
    }

    public ExecutorService getPool() {
        return pool;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        log.error(e.toString(), e);
    }
}
