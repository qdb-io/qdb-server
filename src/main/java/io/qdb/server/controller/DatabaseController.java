package io.qdb.server.controller;

import io.qdb.server.JsonService;
import io.qdb.server.queue.QueueManager;
import io.qdb.server.ServerId;
import io.qdb.server.model.Database;
import io.qdb.server.model.OptLockException;
import io.qdb.server.model.Repository;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.List;

@Singleton
public class DatabaseController extends CrudController {

    private final Repository repo;
    private final QueueManager queueManager;
    private final ServerId serverId;

    public static class DatabaseDTO {

        public String id;
        public Integer version;
        public String owner;

        public DatabaseDTO() { }

        public DatabaseDTO(Database db) {
            id = db.getId();
            version = db.getVersion();
            owner = db.getOwner();
        }
    }

    @Inject
    public DatabaseController(Repository repo, JsonService jsonService, QueueManager queueManager, ServerId serverId) {
        super(jsonService);
        this.repo = repo;
        this.queueManager = queueManager;
        this.serverId = serverId;
    }

    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        List<Database> list = repo.findDatabasesVisibleTo(call.getUser(), offset, limit);
        DatabaseDTO[] ans = new DatabaseDTO[list.size()];
        for (int i = 0; i < ans.length; i++) ans[i] = new DatabaseDTO(list.get(i));
        call.setJson(ans);
    }

    @Override
    protected void count(Call call) throws IOException {
        call.setJson(new Count(repo.countDatabasesVisibleTo(call.getUser())));
    }

    @Override
    protected void show(Call call, String id) throws IOException {
        Database db = repo.findDatabase(id);
        if (db == null) {
            call.setCode(404);
        } else if (db.isVisibleTo(call.getUser())) {
            call.setJson(new DatabaseDTO(db));
        } else {
            call.setCode(403);
        }
    }

    @Override
    protected void create(Call call) throws IOException {
        if (call.getUser().isAdmin()) {
            DatabaseDTO dto = getBodyObject(call, DatabaseDTO.class);
            Database db = new Database();
            db.setId(dto.id);
            db.setOwner(dto.owner);
            call.setCode(201, new DatabaseDTO(repo.createDatabase(db)));
        } else {
            call.setCode(403);
        }
    }

    @Override
    protected void update(Call call, String id) throws IOException {
        Database db = repo.findDatabase(id);
        if (call.getUser().isAdmin() || db != null && call.getUser().getId().equals(db.getOwner())) {
            if (db == null) call.setCode(404);
            else update(db, getBodyObject(call, DatabaseDTO.class), call);
        } else {
            call.setCode(403);
        }
    }

    private void update(Database db, DatabaseDTO dto, Call call) throws IOException {
        if (dto.version != null && !dto.version.equals(db.getVersion())) {
            call.setCode(409, new DatabaseDTO(db));
            return;
        }
        if (dto.owner != null) {
            if (repo.findUser(dto.owner) == null) {
                call.setCode(400, "owner [" + dto.owner + "] does not exist");
                return;
            }
            db.setOwner(dto.owner);
        }
        try {
            call.setJson(new DatabaseDTO(repo.updateDatabase(db)));
        } catch (OptLockException e) {
            db = repo.findDatabase(db.getId());
            if (db == null) call.setCode(410);
            else call.setCode(409, new DatabaseDTO(db));
        }
    }

    @Override
    protected Controller getController(Call call, String id, String resource) throws IOException {
        Database db = repo.findDatabase(id);
        if (db != null) {
            if (!db.isVisibleTo(call.getUser())) return StatusCodeController.SC_403;
            if ("queues".equals(resource)) return new QueueController(jsonService, repo, queueManager, serverId, db);
        }
        return StatusCodeController.SC_404;
    }
}
