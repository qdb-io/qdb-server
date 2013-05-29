package io.qdb.server.repo;

import io.qdb.kvstore.KeyValueStore;
import io.qdb.server.model.*;
import io.qdb.server.model.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * Uses a {@link KeyValueStore} to keep our meta data in memory with periodic snapshots written to disk.
 */
@Singleton
public class RepositoryImpl implements Repository {

    @SuppressWarnings("UnusedDeclaration")
    private static final Logger log = LoggerFactory.getLogger(RepositoryImpl.class);

    private final KeyValueStore<String, ModelObject> store;
    private final ConcurrentMap<String, User> users;
    private final ConcurrentMap<String, Database> databases;
    private final ConcurrentMap<String, Queue> queues;

    @Inject
    public RepositoryImpl(KeyValueStore<String, ModelObject> store,
                @Named("initialAdminPassword") String initialAdminPassword) throws IOException {
        this.store = store;
        users = store.getMap("users", User.class);
        databases = store.getMap("databases", Database.class);
        queues = store.getMap("queues", Queue.class);

        if (findUser("admin") == null) {
            User admin = new User();
            admin.setId("admin");
            admin.setPassword(initialAdminPassword);
            admin.setAdmin(true);
            updateUser(admin);
            log.info("Created initial admin user");
            store.saveSnapshot();
        }
    }

    @Override
    public void close() throws IOException {
        store.close();
    }

    @Override
    public User findUser(String id) throws IOException {
        return users.get(id);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<User> findUsers(int offset, int limit) throws IOException {
        return find(users, offset, limit);
    }

    @Override
    public int countUsers() throws IOException {
        return users.size();
    }

    @Override
    public void updateUser(User user) throws IOException {
        users.put(user.getId(), user);
    }

    @Override
    public Database findDatabase(String id) throws IOException {
        return databases.get(id);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Database> findDatabasesVisibleTo(User user, int offset, int limit) throws IOException {
        if (user.isAdmin()) {
            return find(databases, offset, limit);
        } else {
            ArrayList<Database> ans = new ArrayList<Database>();
            String[] databases = user.getDatabases();
            if (databases != null) {
                for (int i = offset, n = Math.min(offset + limit, databases.length); i < n; i++) {
                    Database db = findDatabase(databases[i]);
                    if (db != null) ans.add(db);
                }
            }
            return ans;
        }
    }

    @Override
    public int countDatabasesVisibleTo(User user) throws IOException {
        if (user.isAdmin()) {
            return databases.size();
        } else {
            String[] databases = user.getDatabases();
            return databases == null ? 0 : databases.length;
        }
    }

    @Override
    public void updateDatabase(Database db) throws IOException {
        databases.put(db.getId(), db);
    }

    @Override
    public Queue findQueue(String id) throws IOException {
        return queues.get(id);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Queue> findQueues(int offset, int limit) throws IOException {
        return find(queues, offset, limit);
    }

    @Override
    public int countQueues() throws IOException {
        return queues.size();
    }

    @Override
    public void updateQueue(Queue queue) throws IOException {
        queues.put(queue.getId(), queue);
    }

    @SuppressWarnings("unchecked")
    private List find(ConcurrentMap map, int offset, int limit) {
        List list = new ArrayList(map.values());
        Collections.sort(list);
        if (limit < 0) limit = Integer.MAX_VALUE - offset;
        int n = list.size();
        if (offset == 0 && limit >= n) return list;
        return offset >= n ? Collections.EMPTY_LIST : list.subList(offset, Math.min(limit, n));
    }
}
