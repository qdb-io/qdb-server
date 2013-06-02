package io.qdb.server.repo;

import io.qdb.server.model.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Retrieves and persists our model objects using a {@link io.qdb.kvstore.KeyValueStore}.
 */
public interface Repository extends Closeable {

    public User findUser(String id) throws IOException;

    public void updateUser(User user) throws IOException;

    public List<User> findUsers(int offset, int limit) throws IOException;

    public int countUsers() throws IOException;


    public Database findDatabase(String id) throws IOException;

    public void updateDatabase(Database db) throws IOException;

    public List<Database> findDatabasesVisibleTo(User user, int offset, int limit) throws IOException;

    public int countDatabasesVisibleTo(User user) throws IOException;


    public Queue findQueue(String id) throws IOException;

    public void updateQueue(Queue queue) throws IOException;

    public List<Queue> findQueues(int offset, int limit) throws IOException;

    public int countQueues() throws IOException;

    public void deleteQueue(String id) throws IOException;


    public Output findOutput(String id) throws IOException;

    public void updateOutput(Output output) throws IOException;

    public List<Output> findOutputs(int offset, int limit) throws IOException;

    public int countOutputs() throws IOException;

    public void deleteOutput(String id) throws IOException;

    /**
     * A change to one of the objects we store. These are published on the shared
     * {@link com.google.common.eventbus.EventBus}.
     */
    public static class ObjectEvent {

        public enum Type { CREATED, UPDATED, DELETED }

        public final Type type;
        public final ModelObject value;

        public ObjectEvent(Type type, ModelObject value) {
            this.type = type;
            this.value = value;
        }

        @Override
        public String toString() {
            return type + " " + value;
        }
    }
}
