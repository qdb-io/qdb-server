package io.qdb.server

import spock.lang.Specification

import spock.lang.Shared

class StandaloneBase extends Specification {

    @Shared TestServer server = new TestServer("build/test-data")
    @Shared Client client = new Client("http://127.0.0.1:9554")

    def setupSpec() {
    }

    def cleanupSpec() {
        server?.close()
    }

    Client.Response GET(String path, String user = "admin", String password = "admin") {
        return client.GET(path, user, password)
    }

    Client.Response POST(String path, Object data, String user = "admin", String password = "admin") {
        return client.POST(path, data, user, password)
    }

    Client.Response POST(String path, Object data, boolean asFormParams, String user = "admin", String password = "admin") {
        return client.POST(path, data, asFormParams, user, password)
    }

    Client.Response POST(String path, String contentType, byte[] data, String user = "admin", String password = "admin") {
        return client.POST(path, contentType, data, user, password)
    }

    Client.Response PUT(String path, Object data, String user = "admin", String password = "admin") {
        return client.PUT(path, data, user, password)
    }

    Client.Response DELETE(String path, String user = "admin", String password = "admin") {
        return client.DELETE(path, user, password)
    }
}
