package io.qdb.server.controller;

import io.qdb.server.JsonService;
import io.qdb.server.model.Database;
import io.qdb.server.model.OptLockException;
import io.qdb.server.model.Repository;
import io.qdb.server.model.User;
import org.apache.zookeeper.KeeperException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class UserController extends CrudController {

    private final Repository repo;

    public static class UserDTO {

        public String id;
        public Integer version;
        public String password;
        public Boolean admin;
        public String[] databases;

        public UserDTO() { }

        public UserDTO(User u) {
            id = u.getId();
            version = u.getVersion();
            admin = u.isAdmin();
            databases = u.getDatabases();
        }
    }

    @Inject
    public UserController(Repository repo, JsonService jsonService) {
        super(jsonService);
        this.repo = repo;
    }

    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        if (call.getUser().isAdmin()) {
            List<User> users = repo.findUsers(offset, limit);
            UserDTO[] ans = new UserDTO[users.size()];
            for (int i = 0; i < ans.length; i++) ans[i] = new UserDTO(users.get(i));
            call.setJson(ans);
        } else {
            call.setCode(403);
        }
    }

    @Override
    protected void count(Call call) throws IOException {
        if (call.getUser().isAdmin()) {
            call.setJson(new Count(repo.countUsers()));
        } else {
            call.setCode(403);
        }
    }

    @Override
    protected void show(Call call, String id) throws IOException {
        User cu = call.getUser();
        if ("me".equals(id)) id = cu.getId();
        if (cu.isAdmin() || cu.getId().equals(id)) {
            User user = repo.findUser(id);
            if (user == null) {
                call.setCode(404);
            } else {
                call.setJson(new UserDTO(user));
            }
        } else {
            call.setCode(403);
        }
    }

    @Override
    protected void create(Call call) throws IOException {
        if (call.getUser().isAdmin()) {
            UserDTO dto = getBodyObject(call, UserDTO.class);
            User u = new User();
            u.setId(dto.id);
            u.setPassword(dto.password);
            if (dto.admin != null) u.setAdmin(dto.admin);
            u.setDatabases(dto.databases);
            call.setJson(new UserDTO(repo.createUser(u)));
        } else {
            call.setCode(403);
        }
    }

    @Override
    protected void update(Call call, String id) throws IOException {
        if (call.getUser().isAdmin()) {
            User u = repo.findUser(id);
            if (u == null) call.setCode(404);
            else update(u, getBodyObject(call, UserDTO.class), call);
        } else {
            call.setCode(403);
        }
    }

    private void update(User u, UserDTO dto, Call call) throws IOException {
        if (dto.version != null && !dto.version.equals(u.getVersion())) {
            call.setCode(409, new UserDTO(u));
            return;
        }
        if (dto.admin != null) u.setAdmin(dto.admin);
        if (dto.databases != null) {
            for (int i = 0; i < dto.databases.length; i++) {
                String db = dto.databases[i];
                if (repo.findDatabase(db) == null) {
                    call.setCode(400, "database [" + db + "] does not exist");
                    return;
                }
            }
            u.setDatabases(dto.databases);
        }
        try {
            call.setJson(new UserDTO(repo.updateUser(u)));
        } catch (OptLockException e) {
            u = repo.findUser(u.getId());
            if (u == null) call.setCode(410);
            else call.setCode(409, new UserDTO(u));
        }
    }

}
