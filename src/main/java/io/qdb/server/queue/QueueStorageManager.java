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

package io.qdb.server.queue;

import io.qdb.server.Util;
import io.qdb.server.model.Database;
import io.qdb.server.model.Queue;
import io.qdb.server.repo.Repository;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;

/**
 * Keeps track of where we can store queue data (one or more paths on the file system) and allocates storage
 * for queues.
 */
@Singleton
public class QueueStorageManager {

    private final File[] queueDataDirs;
    private final Repository repository;

    @Inject
    public QueueStorageManager(@Named("dataDir") String dataDir, Repository repository) throws IOException {
        this.repository = repository;
        queueDataDirs = new File[]{new File(dataDir, "queues")};
        for (File dir : queueDataDirs) Util.ensureDirectory(dir);
    }

    /**
     * If q already exists i.e. there is an existing directory for it then return that directory. Otherwise allocate
     * a new directory and return it.
     */
    public File findDir(Queue q) throws IOException {
        Database db = repository.findDatabase(q.getDatabase());
        if (db == null) {
            throw new IllegalStateException("database [" + q.getDatabase() + "] for queue [" + q.getId() +
                    "] not found");
        }
        String name = db.getQueueForQid(q.getId());
        if (name == null) {
            throw new IllegalStateException("database [" + q.getDatabase() + "] does not have name for queue [" +
                    q.getId() + "]");
        }
        for (File dir : queueDataDirs) {
            File dbDir = new File(dir, q.getDatabase());
            if (dbDir.isDirectory()) {
                File f = new File(dbDir, name);
                if (f.exists()) return f;
            }
        }
        return new File(Util.ensureDirectory(new File(allocateDataDir(q), q.getDatabase())), name);
    }

    private File allocateDataDir(Queue q) {
        if (queueDataDirs.length == 1) return queueDataDirs[0];
        long maxUsableSpace = -1;
        File ans = null;
        for (File dir : queueDataDirs) {
            long usableSpace = dir.getUsableSpace();
            if (usableSpace > maxUsableSpace) {
                ans = dir;
                maxUsableSpace = usableSpace;
            }
        }
        return ans;
    }

}
