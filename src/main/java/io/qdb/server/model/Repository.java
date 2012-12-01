package io.qdb.server.model;

import java.io.IOException;
import java.util.List;

/**
 * Retrieves and persists our model objects. Find methods that return single objects return null if the object
 * does not exist.
 */
public interface Repository {

    enum Status { DISCONNECTED, CONNECTED, CONNECTED_READ_ONLY }

    public Status getStatus() throws IOException;

    public User findUser(String id) throws IOException;

    public List<Database> findDatabasesVisibleTo(User user) throws IOException;

    public Database findDatabase(String id) throws IOException;

    public List<Queue> findQueues(Database db) throws IOException;

    public Queue findQueue(Database db, String nameOrId) throws IOException;

}
