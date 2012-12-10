package io.qdb.server.model;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Retrieves and persists our model objects. Find methods that return single objects return null if the object
 * does not exist. Fires Status instances on the shared EventBus on connect/disconnect events.
 */
public interface Repository {

    public static class Status {
        public Date upSince;
        public boolean isUp() { return upSince != null; }
    }

    public Status getStatus();

    /**
     * Publish information about node to the cluster.
     */
    public void createQdbNode(QdbNode node) throws IOException;

    /**
     * Find all nodes in our cluster that are up.
     */
    public List<QdbNode> findQdbNodes() throws IOException;

    public User findUser(String id) throws IOException;

    public void createUser(User user) throws IOException;

    public List<User> findUsers(int offset, int limit) throws IOException;

    public int countUsers() throws IOException;

    public List<Database> findDatabasesVisibleTo(User user) throws IOException;

    public Database findDatabase(String id) throws IOException;

    public List<Queue> findQueues(Database db) throws IOException;

    public Queue findQueue(Database db, String nameOrId) throws IOException;

}
