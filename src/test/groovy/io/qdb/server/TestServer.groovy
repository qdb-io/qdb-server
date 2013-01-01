package io.qdb.server

import com.google.inject.Injector
import io.qdb.server.zk.EmbeddedZooKeeper
import org.simpleframework.transport.connect.Connection
import org.apache.commons.io.FileUtils
import com.google.inject.Guice
import com.google.inject.util.Modules
import io.qdb.server.model.Repository
import io.qdb.server.queue.QueueManager

/**
 * An in-process QDB server for testing.
 */
class TestServer implements Closeable {

    private Injector injector
    private EmbeddedZooKeeper zookeeper
    private Connection qdb

    TestServer(String dir = "build/test-data", int instance = 0) {
        File dataDir = new File(dir)
        if (dataDir.exists() && dataDir.isDirectory()) FileUtils.deleteDirectory(dataDir)
        if (!dataDir.mkdirs()) throw new IOException("Unable to create [" + dataDir.absolutePath + "]")

        injector = Guice.createInjector(Modules.override(new QdbServerModule()).with(
                instance ? new ClusteredTestModule(dataDir, instance) : new StandaloneTestModule(dataDir)))
        zookeeper = injector.getInstance(EmbeddedZooKeeper.class)
        qdb = injector.getInstance(Connection.class)
    }

    void waitForRepo() {
        Repository repo = injector.getInstance(Repository.class)
        synchronized (repo) {
            for (int i = 0; i < 3 && !repo.getStatus().up; i++) repo.wait(1000);
        }
    }

    @Override
    void close() {
        injector.getInstance(QueueManager)?.close()
        qdb?.close()
        zookeeper?.close()
    }

}
