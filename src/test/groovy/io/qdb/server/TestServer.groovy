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
