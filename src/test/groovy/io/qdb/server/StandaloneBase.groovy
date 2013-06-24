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

    Client.Response GET(String path, String user = "admin", String password = "secret") {
        return client.GET(path, user, password)
    }

    Client.Response POST(String path, Object data, String user = "admin", String password = "secret") {
        return client.POST(path, data, user, password)
    }

    Client.Response POST(String path, Object data, boolean asFormParams, String user = "admin", String password = "secret") {
        return client.POST(path, data, asFormParams, user, password)
    }

    Client.Response POST(String path, String contentType, byte[] data, String user = "admin", String password = "secret") {
        return client.POST(path, contentType, data, user, password)
    }

    Client.Response PUT(String path, Object data, String user = "admin", String password = "secret") {
        return client.PUT(path, data, user, password)
    }

    Client.Response DELETE(String path, String user = "admin", String password = "secret") {
        return client.DELETE(path, user, password)
    }
}
