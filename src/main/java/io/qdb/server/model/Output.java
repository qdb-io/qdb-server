package io.qdb.server.model;

import java.util.Map;

/**
 * A queue output. Outputs push messages coming into a queue somewhere else (e.g. to RabbitMQ).
 */
public class Output extends ModelObject {

    private String queue;
    private String type;
    private String url;
    private boolean enabled;
    private long messageId;
    private long at;
    private int updateIntervalMs;
    private Map<String, Object> params;

    public Output() {
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

    public long getMessageId() {
        return messageId;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }

    public long getAt() {
        return at;
    }

    public void setAt(long at) {
        this.at = at;
    }

    public int getUpdateIntervalMs() {
        return updateIntervalMs;
    }

    public void setUpdateIntervalMs(int updateIntervalMs) {
        this.updateIntervalMs = updateIntervalMs;
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
}
