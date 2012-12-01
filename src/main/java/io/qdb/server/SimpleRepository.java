package io.qdb.server;

import io.qdb.server.model.Database;
import io.qdb.server.model.Repository;
import io.qdb.server.model.User;
import org.codehaus.jackson.type.TypeReference;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple repo for development using data from json files in etc. Later this info will be stored in ZooKeeper.
 */
@Singleton
public class SimpleRepository implements Repository {

    private final JsonService jsonService;
    private final List<User> users;
    private final List<Database> databases;

    @Inject
    public SimpleRepository(JsonService jsonService) throws IOException {
        this.jsonService = jsonService;
        users = load("users", new TypeReference<List<User>>(){});
        databases = load("databases", new TypeReference<List<Database>>(){});
    }

    private <T> T load(String filename, TypeReference typeRef) throws IOException {
        InputStream in = new FileInputStream(new File("etc", filename + ".json"));
        try {
            return jsonService.fromJson(in, typeRef);
        } finally {
            in.close();
        }
    }

    @Override
    public User findUser(String id) {
        for (User user : users) {
            if (user.getId().equals(id)) return user;
        }
        return null;
    }

    @Override
    public List<Database> findDatabasesVisibleTo(User user) {
        List<Database> ans = new ArrayList<Database>();
        for (Database db : databases) {
            if (db.isVisibleTo(user)) ans.add(db);
        }
        return ans;
    }
}
