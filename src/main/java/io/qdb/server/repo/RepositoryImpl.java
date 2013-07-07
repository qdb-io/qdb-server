/*
 * Copyright 2013 David Tinker
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    private final ConcurrentMap<String, Output> outputs;

    @Inject
    public RepositoryImpl(KeyValueStore<String, ModelObject> store,
                @Named("initialAdminPassword") String initialAdminPassword) throws IOException {
        this.store = store;
        users = store.getMap("users", User.class);
        databases = store.getMap("databases", Database.class);
        queues = store.getMap("queues", Queue.class);
        outputs = store.getMap("outputs", Output.class);

        if (findDatabase("default") == null) updateDatabase(new Database("default"));

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
    public synchronized void deleteUser(String id) throws IOException {
        users.remove(id);
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
    public synchronized void deleteDatabase(String id) throws IOException {
        Database db = databases.get(id);
        if (db == null) return;
        Map<String, String> queues = db.getQueues();
        if (queues != null) {
            for (String qid : queues.values()) deleteQueueImpl(qid, true);
        }
        databases.remove(id);
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

    @Override
    public synchronized void deleteQueue(String id) throws IOException {
        deleteQueueImpl(id, false);
    }

    private void deleteQueueImpl(String id, boolean ignoreDatabase) {
        Queue q = queues.get(id);
        if (q == null) return;
        if (!ignoreDatabase) {
            Database db = databases.get(q.getDatabase());
            if (db != null) {
                String dq = db.getQueueForQid(id);
                if (dq != null) {
                    db = db.deepCopy();
                    db.getQueues().remove(dq);
                    databases.put(db.getId(), db);
                }
            }
        }
        Map<String, String> outputs = q.getOutputs();
        if (outputs != null) {
            for (Map.Entry<String, String> e : outputs.entrySet()) outputs.remove(e.getValue());
        }
        queues.remove(id);
    }

    @Override
    public Output findOutput(String id) throws IOException {
        return outputs.get(id);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Output> findOutputs(int offset, int limit) throws IOException {
        return find(outputs, offset, limit);
    }

    @Override
    public int countOutputs() throws IOException {
        return outputs.size();
    }

    @Override
    public void updateOutput(Output output) throws IOException {
        outputs.put(output.getId(), output);
    }

    @Override
    public synchronized void deleteOutput(String id) throws IOException {
        Output o = outputs.get(id);
        if (o == null) return;
        Queue q = queues.get(o.getQueue());
        if (q != null) {
            String qo = q.getOutputForOid(id);
            if (qo != null) {
                q = q.deepCopy();
                q.getOutputs().remove(qo);
                queues.put(q.getId(), q);
            }
        }
        outputs.remove(id);
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
