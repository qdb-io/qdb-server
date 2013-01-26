package io.qdb.server.repo;

import io.qdb.kvstore.KeyValueStore;
import io.qdb.kvstore.OptimisticLockingException;
import io.qdb.server.model.*;
import io.qdb.server.model.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * Keeps our meta data in maps in memory with periodic snapshots written to disk. Uses a MessageBuffer as a tx log
 * for replay from the last snapshot after a crash.
 */
@Singleton
public class RepositoryImpl implements Repository {

    @SuppressWarnings("UnusedDeclaration")
    private static final Logger log = LoggerFactory.getLogger(RepositoryImpl.class);

    private final KeyValueStore<String, ModelObject> store;
    private final ConcurrentMap<String, User> users;
    private final ConcurrentMap<String, Database> databases;
    private final ConcurrentMap<String, Queue> queues;

    private Date upSince;

    @Inject
    public RepositoryImpl(KeyValueStore<String, ModelObject> store) throws IOException {
        this.store = store;
        users = store.getMap("users", User.class);
        databases = store.getMap("databases", Database.class);
        queues = store.getMap("queues", Queue.class);
        upSince = new Date();
    }

    @Override
    public void close() throws IOException {
        store.close();
    }

    @Override
    public String getRepositoryId() {
        return store.getStoreId();
    }

    @Override
    public Status getStatus() {
        Status s = new Status();
        s.upSince = upSince;
        return s;
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
    public User createUser(User user) throws IOException {
        create(users, user);
        return user;
    }

    @Override
    public User updateUser(User user) throws IOException {
        update(users, user);
        return user;
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
    public Database createDatabase(Database db) throws IOException {
        create(databases, db);
        return db;
    }

    @Override
    public Database updateDatabase(Database db) throws IOException {
        update(databases, db);
        return db;
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
    public Queue createQueue(Queue queue) throws IOException {
        create(queues, queue);
        return queue;
    }

    @Override
    public Queue updateQueue(Queue queue) throws IOException {
        update(queues, queue);
        return queue;
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

    @SuppressWarnings("unchecked")
    private void create(ConcurrentMap map, ModelObject o) throws IOException {
        if (map.putIfAbsent(o.getId(), o) != null) {
            throw new DuplicateIdException("Duplicate id: " + tos(o));
        }
    }

    @SuppressWarnings("unchecked")
    private void update(ConcurrentMap map, ModelObject o) throws IOException {
        if (map.replace(o.getId(), o) == null) {
            throw new OptimisticLockingException(tos(o) + " not found");
        }
    }

    private static String tos(ModelObject o) {
        return o == null ? "null" : o.getClass().getSimpleName() + ":" + o.getId();
    }
}
