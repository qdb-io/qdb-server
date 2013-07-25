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

import spock.lang.Stepwise

@Stepwise
class QueuesSpec extends StandaloneBase {

    def setupSpec() {
        assert POST("/users/david", [password: "secret"]).code == 201
        assert POST("/db/foo", [owner: "david"]).code == 201
    }

    def "Create queue"() {
        def data = [maxSize: "10m"]
        def ans = POST("/db/foo/q/bar", data, true, "david", "secret")
        def ans2 = POST("/db/foo/q/bar", data, "david", "secret")

        expect:
        ans.code == 201
        ans.json.id == "bar"
        ans.json.maxSize == "10 MB"
        ans.json.maxPayloadSize == "1 MB"
        ans.json.contentType == "application/octet-stream"
        ans2.code == 200
    }

    def "Create queue in default database"() {
        def data = [maxSize: "5m"]
        def ans = POST("/q/oink?borg=true", data)

        expect:
        ans.code == 201
        ans.json.id == "oink"
        ans.json.maxSize == 5 * 1024 * 1024
    }

    def "Queue id validation"() {
        def ans = POST("/db/foo/q/a@b", [:], "david", "secret")

        expect:
        ans.code == 422
    }

    def "List queues"() {
        def ans = GET("/db/foo/q", "david", "secret")

        expect:
        ans.code == 200
        ans.json.size() == 1
        ans.json[0].id == "bar"
    }

    def "List queues in default database"() {
        def ans = GET("/q")

        expect:
        ans.code == 200
        ans.json.size() == 1
        ans.json[0].id == "oink"
    }

    def "Count queues"() {
        def ans = GET("/db/foo/q?count=true", "david", "secret")

        expect:
        ans.code == 200
        ans.json.count == 1
    }

    def "Get queue"() {
        def ans = GET("/db/foo/q/bar", "david", "secret")

        expect:
        ans.code == 200
        ans.json.id == "bar"
    }

    def "Update queue"() {
        def ans = PUT("/db/foo/q/bar?borg=true", [maxSize: 20000000, maxPayloadSize: 100000], "david", "secret")

        expect:
        ans.code == 200
        ans.json.id == "bar"
        ans.json.maxSize == 20000000
        ans.json.maxPayloadSize == 100000
    }

    def "Queue maxSize validation"() {
        def ans = PUT("/db/foo/q/bar", [maxSize: 100000], "david", "secret")

        expect:
        ans.code == 422
    }

    def "Queue maxPayloadSize validation"() {
        def ans = PUT("/db/foo/q/bar", [maxPayloadSize: (int)(20000000 / 3) + 1], "david", "secret")

        expect:
        ans.code == 422
    }

    def "Queue warnAfter"() {
        def ans = PUT("/db/foo/q/bar", [warnAfter: "90"], "david", "secret")

        expect:
        ans.code == 200
        ans.json.warnAfter == "0:01:30"
    }

    def "Queue errorAfter"() {
        def ans = PUT("/db/foo/q/bar", [errorAfter: "80"], "david", "secret")

        expect:
        ans.code == 200
        ans.json.errorAfter == "0:01:20"
    }

    def "Delete queue"() {
        def ans = DELETE("/db/foo/q/bar", "david", "secret")
        def ans2 = GET("/db/foo/q/bar")

        expect:
        ans.code == 200
        ans2.code == 404
    }

    def "Delete queue with outputs"() {
        assert POST("/db/foo/q/bar", [maxSize: 1000000]).code == 201
        assert POST("/db/foo/q/bar/out/rabbit", [type: "rabbitmq"]).code == 201
        def ans = DELETE("/db/foo/q/bar")
        def ans2 = GET("/db/foo/q/bar/out/rabbit")
        def ans3 = GET("/db/foo/q/bar")

        expect:
        ans.code == 200
        ans2.code == 404
        ans3.code == 404
    }
}
