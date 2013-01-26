package io.qdb.server.model;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Retrieves and persists our model objects using a {@link io.qdb.kvstore.KeyValueStore}.
 */
public interface Repository extends Closeable {

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
        public String clusterName;
        public ServerStatus master;
        public ServerStatus[] servers;
        public String serverDiscoveryStatus;
        public String masterElectionStatus;

        public boolean isUp() { return upSince != null; }
    }

    public enum ServerRole {
        MASTER, SLAVE
    }

    public static class ServerStatus implements Comparable<ServerStatus> {
        public final String id;
        public ServerRole role;
        public boolean connected;
        public Integer msSinceLastContact;
        public String message;

        public ServerStatus(String id, ServerRole role, Integer msSinceLastContact, String message) {
            this.id = id;
            this.role = role;
            this.msSinceLastContact = msSinceLastContact;
            this.message = message;
        }

        @Override
        public int compareTo(ServerStatus o) {
            return id.compareTo(o.id);
        }
    }

    /**
     * Get this repository's unique id.
     */
    public String getRepositoryId();

    public Status getStatus();


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
