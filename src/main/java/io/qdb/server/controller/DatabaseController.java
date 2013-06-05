package io.qdb.server.controller;

import io.qdb.server.model.Database;
import io.qdb.server.repo.Repository;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.List;

@Singleton
public class DatabaseController extends CrudController {

    private final Repository repo;
    private final QueueController queueController;

    public static class DatabaseDTO {

        public String id;
        public Integer version;
        public String owner;

        @SuppressWarnings("UnusedDeclaration")
        public DatabaseDTO() { }

        public DatabaseDTO(Database db) {
            id = db.getId();
            version = db.getVersion();
            owner = db.getOwner();
        }
    }

    @Inject
    public DatabaseController(Repository repo, JsonService jsonService, QueueController queueController) {
        super(jsonService);
        this.repo = repo;
        this.queueController = queueController;
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
    protected void createOrUpdate(Call call, String id) throws IOException {
        if (call.getUser().isAdmin()) {
            DatabaseDTO dto = getBodyObject(call, DatabaseDTO.class);
            Database db;
            boolean create;
            synchronized (repo) {
                db = repo.findDatabase(id);
                if (create = db == null) {
                    if (call.isPut()) {
                        call.setCode(404);
                        return;
                    }
                    db = new Database(id);
                } else {
                    if (dto.version != null && !dto.version.equals(db.getVersion())) {
                        call.setCode(409, new DatabaseDTO(db));
                        return;
                    }
                    db = (Database)db.clone();
                }

                boolean changed = create;
                if (dto.owner != null && !dto.owner.equals(db.getOwner())) {
                    if (repo.findUser(dto.owner) == null) {
                        call.setCode(400, "owner [" + dto.owner + "] does not exist");
                        return;
                    }
                    db.setOwner(dto.owner);
                    changed = true;
                }

                if (changed) repo.updateDatabase(db);
            }
            call.setCode(create ? 201 : 200, new DatabaseDTO(db));
        } else {
            call.setCode(403);
        }
    }

    @Override
    protected void delete(Call call, String id) throws IOException {
        synchronized (repo) {
            Database db = repo.findDatabase(id);
            if (db == null) {
                call.setCode(404);
                return;
            }
            repo.deleteDatabase(id);
        }
    }

    @Override
    protected Controller getController(Call call, String id, String resource) throws IOException {
        Database db = repo.findDatabase(id);
        if (db != null) {
            if (!db.isVisibleTo(call.getUser())) return StatusCodeController.SC_403;
            call.setDatabase(db);
            if ("queues".equals(resource)) return queueController;
        }
        return StatusCodeController.SC_404;
    }
}
