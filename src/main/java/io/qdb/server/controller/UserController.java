package io.qdb.server.controller;

import io.qdb.server.repo.Repository;
import io.qdb.server.model.User;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Arrays;
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
    protected void createOrUpdate(Call call, String id) throws IOException {
        if (call.getUser().isAdmin()) {
            UserDTO dto = getBodyObject(call, UserDTO.class);
            User u;
            boolean create;
            synchronized (repo) {
                u = repo.findUser(id);
                if (create = u == null) {
                    if (call.isPut()) {
                        call.setCode(404);
                        return;
                    }
                    u = new User(id);
                } else {
                    if (dto.version != null && !dto.version.equals(u.getVersion())) {
                        call.setCode(409, new UserDTO(u));
                        return;
                    }
                    u = (User)u.clone();
                }

                boolean changed = create;
                if (dto.password != null && !u.doesPasswordMatch(dto.password)) {
                    u.setPassword(dto.password);
                    changed = true;
                }
                if (dto.admin != null && dto.admin != u.isAdmin()) {
                    u.setAdmin(dto.admin);
                    changed = true;
                }
                if (dto.databases != null && !Arrays.equals(dto.databases, u.getDatabases())) {
                    for (int i = 0; i < dto.databases.length; i++) {
                        String db = dto.databases[i];
                        if (repo.findDatabase(db) == null) {
                            call.setCode(400, "database [" + db + "] does not exist");
                            return;
                        }
                    }
                    u.setDatabases(dto.databases);
                    changed = true;
                }

                if (changed) repo.updateUser(u);
            }
            call.setCode(create ? 201 : 200, new UserDTO(u));
        } else {
            call.setCode(403);
        }
    }

    @Override
    protected void delete(Call call, String id) throws IOException {
        synchronized (repo) {
            User u = repo.findUser(id);
            if (u == null) {
                call.setCode(404);
                return;
            }
            repo.deleteUser(id);
        }
    }
}
