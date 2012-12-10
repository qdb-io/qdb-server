package io.qdb.server.controller;

import io.qdb.server.JsonService;
import io.qdb.server.model.Repository;
import io.qdb.server.model.User;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

@Singleton
public class UserController extends CrudController {

    private final Repository repo;
    private final JsonService jsonService;

    @Inject
    public UserController(Repository repo, JsonService jsonService) {
        this.repo = repo;
        this.jsonService = jsonService;
    }

    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        if (call.getUser().isAdmin()) {
            ListResult res = new ListResult(offset, limit, repo.findUsers(offset, limit));
            if (res.total < 0) res.total = repo.countUsers();
            call.setJson(res);
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

    @Override
    protected void create(Call call) throws IOException {
        if (call.getUser().isAdmin()) {
            InputStream ins = Channels.newInputStream(call.getRequest().getByteChannel());
            try {
                User user = jsonService.fromJson(ins, User.class);
                repo.createUser(user);
                call.setJson(user);
            } finally {
                ins.close();
            }
        } else {
            call.setCode(403);
        }
    }
}
