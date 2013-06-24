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

package io.qdb.server

import com.google.inject.Injector
import org.simpleframework.transport.connect.Connection
import org.apache.commons.io.FileUtils
import com.google.inject.Guice
import com.google.inject.util.Modules
import io.qdb.server.queue.QueueManager

/**
 * An in-process QDB server for testing.
 */
class TestServer implements Closeable {

    private Injector injector
    private Connection qdb

    TestServer(String dir = "build/test-data") {
        File dataDir = new File(dir)
        if (dataDir.exists() && dataDir.isDirectory()) FileUtils.deleteDirectory(dataDir)
        if (!dataDir.mkdirs()) throw new IOException("Unable to create [" + dataDir.absolutePath + "]")

        injector = Guice.createInjector(Modules.override(new QdbServerModule()).with(new StandaloneTestModule(dataDir)))
        qdb = injector.getInstance(Connection)
    }

    @Override
    void close() {
        injector.getInstance(QueueManager)?.close()
        qdb?.close()
    }

}
