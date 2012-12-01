package io.qdb.server.model;

import java.util.List;

/**
 * Retrieves and persists our model objects. Find methods that return single objects return null if the object is
 * does not exist.
 */
public interface Repository {

    public User findUser(String id);

    public List<Database> findDatabasesVisibleTo(User user);

    public Database findDatabase(String id);

    public List<Queue> findQueues(Database db);

}
