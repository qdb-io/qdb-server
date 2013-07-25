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

package io.qdb.server;

import java.io.File;
import java.io.IOException;

/**
 * Static utility methods.
 */
public class Util {

    /**
     * Ensure that dir exists, creating it if needed and that it is a writeable directory.
     */
    public static File ensureDirectory(File dir) throws IOException {
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException("Unable to create directory [" + dir.getAbsolutePath() + "]");
            }
        } else if (!dir.isDirectory()) {
            throw new IOException("Not a directory [" + dir.getAbsolutePath() + "]");
        } else if (!dir.canWrite()) {
            throw new IOException("Not writable [" + dir.getAbsolutePath() + "]");
        }
        return dir.getAbsoluteFile();
    }
}
