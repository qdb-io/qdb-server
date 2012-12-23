package io.qdb.server.model;

/**
 * A queue.
 */
public class Queue extends ModelObject {

    private String database;

    public Queue() {
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    @Override
    public String toString() {
        return super.toString() + ":" + database;
    }
}
