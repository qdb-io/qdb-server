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
import io.qdb.server.model.Queue;

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

    @Inject
    public QueueStorageManager(@Named("dataDir") String dataDir) throws IOException {
        queueDataDirs = new File[]{new File(dataDir, "queues")};
        for (File dir : queueDataDirs) Util.ensureDirectory(dir);
    }

    /**
     * If q already exists i.e. there is an existing directory for it then return that directory. Otherwise allocate
     * a new directory and return it.
     */
    public File findDir(Queue q) {
        for (File dir : queueDataDirs) {
            File f = new File(dir, q.getId());
            if (f.exists()) return f;
        }
        return new File(allocateDataDir(q), q.getId());
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
