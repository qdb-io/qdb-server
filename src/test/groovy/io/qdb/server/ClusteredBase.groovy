package io.qdb.server

import spock.lang.Shared
import spock.lang.Specification

class ClusteredBase extends Specification {

    @Shared TestServer[] servers = new TestServer[3];
    @Shared Client[] clients = new Client[3];

    def setupSpec() {
//        for (int i = 0; i < servers.length; i++) {
//            servers[i] = new TestServer("build/test-data" + (i + 1), i + 1)
//            clients[i] = new Client("http://127.0.0.1:" + (9554 + i))
//        }
//        servers.each { it.waitForRepo() }
    }

    def cleanupSpec() {
        servers.each { it?.close() }
    }

    Client.Response GET(int i, String path, String user = "admin", String password = "admin") {
        return clients[i - 1].GET(path, user, password)
    }

    Client.Response POST(int i, String path, Object data, String user = "admin", String password = "admin") {
        return clients[i - 1].POST(path, data, user, password)
    }

    Client.Response POST(int i, String path, String contentType, byte[] data, String user = "admin", String password = "admin") {
        return clients[i - 1].POST(path, contentType, data, user, password)
    }

    Client.Response PUT(int i, String path, Object data, String user = "admin", String password = "admin") {
        return clients[i - 1].PUT(path, data, user, password)
    }
}
