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

package io.qdb.server.controller;

import io.qdb.server.model.Database;
import io.qdb.server.repo.Repository;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

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

    private static final Pattern VALID_DATABASE_ID = Pattern.compile("[0-9a-z\\-_]+", Pattern.CASE_INSENSITIVE);

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
                    if (!VALID_DATABASE_ID.matcher(id).matches()) {
                        call.setCode(422, "Database id must contain only letters, numbers, hyphens and underscores");
                        return;
                    }
                    db = new Database(id);
                } else {
                    if (dto.version != null && !dto.version.equals(db.getVersion())) {
                        call.setCode(409, new DatabaseDTO(db));
                        return;
                    }
                    db = db.deepCopy();
                }

                boolean changed = create;
                if (dto.owner != null && !dto.owner.equals(db.getOwner())) {
                    if (repo.findUser(dto.owner) == null) {
                        call.setCode(422, "owner [" + dto.owner + "] does not exist");
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
            if ("q".equals(resource)) return queueController;
        }
        return StatusCodeController.SC_404;
    }
}
