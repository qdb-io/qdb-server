package io.qdb.server.controller;

import io.qdb.server.model.Repository;
import io.qdb.server.model.User;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

@Singleton
public class UserController extends CrudController {

    private final Repository repo;

    @Inject
    public UserController(Repository repo) {
        this.repo = repo;
    }

    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        if (call.getUser().isAdmin()) {
            call.setJson(new ListResult(offset, limit, repo.countUsers(), repo.findUsers(offset, limit)));
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
                user.setPasswordHash(null);
                call.setJson(user);
            }
        } else {
            call.setCode(403);
        }
    }
}
