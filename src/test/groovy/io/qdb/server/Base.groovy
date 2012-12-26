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

    @Shared private TestServer testServer = new TestServer("build/test-data")
    @Shared private Client client = new Client("http://127.0.0.1:9554")

    def setupSpec() {
    }

    def cleanupSpec() {
        testServer?.close()
    }

    Client.Response GET(String path, String user = "admin", String password = "admin") {
        return client.GET(path, user, password)
    }

    Client.Response POST(String path, Object data, String user = "admin", String password = "admin") {
        return client.POST(path, data, user, password)
    }

    Client.Response PUT(String path, Object data, String user = "admin", String password = "admin") {
        return client.PUT(path, data, user, password)
    }
}
