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

import io.qdb.buffer.MessageBuffer;
import io.qdb.server.ExpectedIOException;
import io.qdb.server.controller.JsonService;
import io.qdb.server.databind.DataBinder;
import io.qdb.server.model.Database;
import io.qdb.server.model.Input;
import io.qdb.server.model.Queue;
import io.qdb.server.queue.QueueManager;
import io.qdb.server.repo.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;

/**
 * Fetches messages from somewhere and appends them to a queue.
 */
public class InputJob implements Runnable, InputHandler.Sink {

    private static final Logger log = LoggerFactory.getLogger(InputJob.class);

    private final InputManager inputManager;
    private final InputHandlerFactory handlerFactory;
    private final QueueManager queueManager;
    private final Repository repo;
    private final JsonService jsonService;
    private final String inputId;

    private Thread thread;
    private String inputPath;
    private Input input;
    private int errorCount;
    private boolean stopFlag;
    private MessageBuffer buffer;
    private long lastMessageTimestamp;
    private long lastMessageId;

    public InputJob(InputManager inputManager, InputHandlerFactory handlerFactory, QueueManager queueManager,
                    Repository repo, JsonService jsonService, String inputId) {
        this.inputManager = inputManager;
        this.handlerFactory = handlerFactory;
        this.queueManager = queueManager;
        this.repo = repo;
        this.jsonService = jsonService;
        this.inputId = inputId;
    }

    public String getInputId() {
        return inputId;
    }

    @Override
    public void run() {
        thread = Thread.currentThread();
        try {
            mainLoop();
        } catch (Exception x) {
            log.error(this + ": " + x, x);
        } finally {
            if (log.isDebugEnabled()) log.debug(this + " exit");
            inputManager.onInputJobExit(this);
        }
    }

    private void mainLoop() throws Exception {
        while (!isStopFlag()) {

            input = repo.findInput(inputId);
            if (input == null) {
                if (log.isDebugEnabled()) log.debug("Input [" + inputId + "] does not exist");
                return;
            }
            if (!input.isEnabled()) return;

            Queue q = repo.findQueue(input.getQueue());
            if (q == null) {
                if (log.isDebugEnabled()) log.debug("Queue [" + input.getQueue() + "] does not exist");
                return;
            }

            Database db = repo.findDatabase(q.getDatabase());
            if (db == null) {
                if (log.isDebugEnabled()) log.debug("Database [" + q.getDatabase() + "] does not exist");
                return;
            }

            inputPath = toPath(db, q, input);

            InputHandler handler;
            try {
                handler = handlerFactory.createHandler(input.getType());
            } catch (IllegalArgumentException e) {
                log.error("Error creating handler for " + inputPath + ": " + e.getMessage(), e);
                return;
            }

            boolean initOk = false;
            try {
                // give the handler clones so modifications to q or input don't cause trouble
                Input ic = input.deepCopy();
                Queue qc = q.deepCopy();
                Map<String, Object> p = ic.getParams();
                if (p != null) new DataBinder(jsonService).ignoreInvalidFields(true).bind(p, handler).check();
                handler.init(qc, ic, inputPath);
                initOk = true;
            } catch (IllegalArgumentException e) {
                log.error(inputPath + ": " + e.getMessage());
                return;
            } catch (Exception e) {
                log.error(inputPath + ": " + e.getMessage(), e instanceof ExpectedIOException ? null : e);
                ++errorCount;
            }

            try {
                if (initOk) {
                    buffer = queueManager.getBuffer(q);
                    if (buffer == null) {   // we might be busy starting up or something
                        if (log.isDebugEnabled()) log.debug("Queue [" + q.getId() + "] does not have a buffer");
                        ++errorCount;
                    } else {
                        try {
                            fetchMessages(handler);
                        } catch (Exception e) {
                            ++errorCount;
                            log.error(inputPath + ": " + e.getMessage(), e);
                        }
                    }
                }
            } finally {
                try {
                    handler.close();
                } catch (IOException e) {
                    log.error(inputPath + ": Error closing handler: " + e, e);
                }
            }

            // todo use backoff policy from input
            int sleepMs = errorCount * 1000;
            if (sleepMs > 0) {
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ignore) {
                }
            }
        }
    }

    /**
     * Fetch messages using our handler until we are closed or our input is changed by someone else.
     */
    public void fetchMessages(final InputHandler handler) throws Exception {
        if (log.isDebugEnabled()) log.debug(this + ": fetching messages");

        long lastMessageId = input.getLastMessageId();
        this.lastMessageId = lastMessageId;
        this.lastMessageTimestamp = input.getLastMessageTimestamp();

        // start the handler on a separate thread so it can block if it wants to
        inputManager.getPool().execute(new Runnable() {
            @Override
            public void run() {
                if (isStopFlag()) return;
                try {
                    log.debug(InputJob.this + " start");
                    handler.start(InputJob.this);
                } catch (Exception e) {
                    log.error(this + " start failed: " + e, e);
                    stop();
                }
                log.debug(InputJob.this + " start finished");
            }
        });

        long lastUpdate = System.currentTimeMillis();
        int updateIntervalMs = input.getUpdateIntervalMs();

        boolean exitLoop = false;
        while (!exitLoop && !isStopFlag()) {
            try {
                Thread.sleep(updateIntervalMs);
            } catch (InterruptedException e) {
                exitLoop = true;
            }

            Input in = repo.findInput(inputId);
            if (in != input) {
                exitLoop = true; // input has been changed by someone else
                errorCount = 0;
            }

            synchronized (this) {
                if (lastMessageId != this.lastMessageId && (exitLoop || updateIntervalMs <= 0
                        || System.currentTimeMillis() - lastUpdate >= updateIntervalMs)) {
                    synchronized (repo) {
                        in = repo.findInput(inputId);
                        input = in.deepCopy();
                        handler.updateInput(input);
                        input.setLastMessageId(lastMessageId = this.lastMessageId);
                        input.setLastMessageTimestamp(this.lastMessageTimestamp);
                        repo.updateInput(input);
                        lastUpdate = System.currentTimeMillis();
                    }
                }
            }
        }
    }

    @Override
    public synchronized void append(String routingKey, byte[] payload) throws IOException {
        long timestamp = System.currentTimeMillis();
        lastMessageId = buffer.append(timestamp, routingKey, payload);
        lastMessageTimestamp = timestamp;
        errorCount = 0;
        if (log.isDebugEnabled()) log.debug(this + " appended id " + lastMessageId + " timestamp " + lastMessageTimestamp);
    }

    @Override
    public synchronized void append(String routingKey, ReadableByteChannel payload, int payloadSize) throws IOException {
        long timestamp = System.currentTimeMillis();
        lastMessageId = buffer.append(timestamp, routingKey, payload, payloadSize);
        lastMessageTimestamp = timestamp;
        errorCount = 0;
        if (log.isDebugEnabled()) log.debug(this + " appended id " + lastMessageId + " timestamp " + lastMessageTimestamp);
    }

    @Override
    public synchronized void error(String msg, Throwable t) {
        log.error(this + ": " + msg,
                t instanceof IllegalArgumentException || t instanceof ExpectedIOException ? null : t);
        if (!(t instanceof IllegalArgumentException)) {
            ++errorCount;
            stop();
        }
    }

    @Override
    public void error(Throwable t) {
        error(t.toString(), t);
    }

    public void inputChanged(Input o) {
        // if o is the same object as our current Input instance then we made the change so don't stop
        if (o != input && thread != null) thread.interrupt();
    }

    public synchronized void stop() {
        stopFlag = true;
        if (thread != null) thread.interrupt();
    }

    private synchronized boolean isStopFlag() {
        return stopFlag;
    }

    /**
     * Create user friendly identifier for the output for error messages and so on.
     */
    private String toPath(Database db, Queue q, Input in) {
        StringBuilder b = new StringBuilder();
        String dbId = db.getId();
        if (!"default".equals(dbId)) b.append("/db/").append(dbId);
        String s = db.getQueueForQid(q.getId());
        if (s != null) {
            b.append("/q/").append(s);
            s = q.getInputForInputId(in.getId());
            if (s != null) {
                return b.append("/in/").append(s).toString();
            }
        }
        return in.toString();
    }

    @Override
    public String toString() {
        return inputPath == null ? "input[" + inputId + "]" : inputPath;
    }
}
