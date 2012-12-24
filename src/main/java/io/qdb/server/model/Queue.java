package io.qdb.server.model;

/**
 * A queue.
 */
public class Queue extends ModelObject {

    private String database;
    private long maxSize;
    private int maxPayloadSize;
    private String contentType;

    public Queue() {
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    public int getMaxPayloadSize() {
        return maxPayloadSize;
    }

    public void setMaxPayloadSize(int maxPayloadSize) {
        this.maxPayloadSize = maxPayloadSize;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    @Override
    public String toString() {
        return super.toString() + ":" + database;
    }
}
