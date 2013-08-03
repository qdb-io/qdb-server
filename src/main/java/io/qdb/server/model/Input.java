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

package io.qdb.server.model;

import java.util.Map;

/**
 * A queue input. Inputs slurp messages from somewhere (e.g. RabbitMQ) and append them to a queue.
 */
public class Input extends ModelObject {

    private String queue;
    private String type;
    private String url;
    private boolean enabled;
    private long lastMessageId;
    private long lastMessageTimestamp;
    private int updateIntervalMs;
    private int warnAfter;
    private int errorAfter;
    private Map<String, Object> params;

    public Input() {
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public long getLastMessageId() {
        return lastMessageId;
    }

    public void setLastMessageId(long lastMessageId) {
        this.lastMessageId = lastMessageId;
    }

    public long getLastMessageTimestamp() {
        return lastMessageTimestamp;
    }

    public void setLastMessageTimestamp(long lastMessageTimestamp) {
        this.lastMessageTimestamp = lastMessageTimestamp;
    }

    public int getUpdateIntervalMs() {
        return updateIntervalMs;
    }

    public void setUpdateIntervalMs(int updateIntervalMs) {
        this.updateIntervalMs = updateIntervalMs;
    }

    public int getWarnAfter() {
        return warnAfter;
    }

    public void setWarnAfter(int warnAfter) {
        this.warnAfter = warnAfter;
    }

    public int getErrorAfter() {
        return errorAfter;
    }

    public void setErrorAfter(int errorAfter) {
        this.errorAfter = errorAfter;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    @Override
    public String toString() {
        return super.toString() + ":queue=" + queue + ":type=" + type;
    }

    public Input deepCopy() {
        Input o = (Input)clone();
        if (params != null) o.params = CLONER.deepClone(params);
        return o;
    }
}
