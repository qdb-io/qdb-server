package io.qdb.server.model;

/**
 * A queue.
 */
public class Queue extends ModelObject {

    private String databaseId;
    private String name;

    public Queue() {
    }

    public String getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(String databaseId) {
        this.databaseId = databaseId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
