package io.qdb.server.model;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Retrieves and persists our model objects. Find methods that return single objects return null if the object
 * does not exist. Fires Status instances on the shared EventBus on connect/disconnect events. Use a negative
 * limit parameter for findXXX(offset,limit) methods to fetch all data.
 */
public interface Repository {

    /**
     * Thrown on all repository operations except getStatus if the repo is down.
     */
    public static class UnavailableException extends IOException {
        public UnavailableException(String msg) {
            super(msg);
        }
    }

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

    public User createUser(User user) throws IOException;

    public User updateUser(User user) throws IOException;

    public List<User> findUsers(int offset, int limit) throws IOException;

    public int countUsers() throws IOException;


    public Database findDatabase(String id) throws IOException;

    public Database createDatabase(Database db) throws IOException;

    public Database updateDatabase(Database db) throws IOException;

    public List<Database> findDatabasesVisibleTo(User user, int offset, int limit) throws IOException;

    public int countDatabasesVisibleTo(User user) throws IOException;


    public Queue findQueue(String id) throws IOException;

    public Queue createQueue(Queue queue) throws IOException;

    public Queue updateQueue(Queue queue) throws IOException;

    public List<Queue> findQueues(int offset, int limit) throws IOException;

    public int countQueues() throws IOException;


}
