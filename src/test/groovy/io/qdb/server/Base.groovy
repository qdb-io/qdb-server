package io.qdb.server

import spock.lang.Specification
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import org.simpleframework.transport.connect.Connection
import com.google.inject.Injector
import com.google.inject.Guice
import spock.lang.Shared
import io.qdb.server.zk.EmbeddedZooKeeper
import io.qdb.server.model.Repository
import org.apache.commons.io.FileUtils
import com.google.inject.util.Modules

class Base extends Specification {

    @Shared private EmbeddedZooKeeper zookeeper;
    @Shared private Connection qdb;
    @Shared private Client client = new Client("http://127.0.0.1:9554")

    def setupSpec() {
        File dataDir = new File("build/test-data")
        if (dataDir.exists() && dataDir.isDirectory()) FileUtils.deleteDirectory(dataDir)
        if (!dataDir.mkdirs()) throw new IOException("Unable to create [" + dataDir.absolutePath + "]")

        Injector injector = Guice.createInjector(Modules.override(new QdbServerModule()).with(new ModuleForTests(dataDir)))
        zookeeper = injector.getInstance(EmbeddedZooKeeper.class)
        Repository repo = injector.getInstance(Repository.class)
        synchronized (repo) {
            for (int i = 0; i < 3 && !repo.getStatus().up; i++) repo.wait(1000);
        }
        qdb = injector.getInstance(Connection.class)
    }

    def cleanupSpec() {
        qdb?.close()
        zookeeper?.close()
    }

    protected GET(String path, String user = "admin", String password = "admin") {
        return client.GET(path, user, password)
    }

    protected POST(String path, Object data, String user = "admin", String password = "admin") {
        return client.POST(path, data, user, password)
    }

    protected PUT(String path, Object data, String user = "admin", String password = "admin") {
        return client.PUT(path, data, user, password)
    }
}
