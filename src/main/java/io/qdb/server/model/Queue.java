package io.qdb.server.model;

/**
 * A queue.
 */
public class Queue extends ModelObject {

    private String database;
    private String contentType;

    public Queue() {
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
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
