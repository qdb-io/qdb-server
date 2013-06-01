package io.qdb.server.model;

/**
 * A queue output. Outputs push messages coming into a queue somewhere else (e.g. to RabbitMQ).
 */
public class Output extends ModelObject {

    private String queue;
    private String type;
    private String url;
    private boolean enabled;
    private long messageId;

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

    @Override
    public String toString() {
        return super.toString() + ":queue=" + queue + ":type=" + type;
    }
}
