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
class OutputsSpec extends StandaloneBase {

    def setupSpec() {
        assert POST("/db/foo", [:]).code == 201
        assert POST("/db/foo/q/bar", [maxSize: 1000000]).code == 201
    }

    def "Create output"() {
        def data = [type: "rabbitmq", url: "amqp://127.0.0.1/"]
        def ans = POST("/db/foo/q/bar/out/rabbit", data)
        def ans2 = POST("/db/foo/q/bar/out/rabbit", data)

        expect:
        ans.code == 201
        ans.json.id == "rabbit"
        ans.json.oid.length() > 0
        ans.json.type == data.type
        ans.json.url == data.url
        ans.json.enabled == true
        ans2.code == 200
    }

    def "Output id validation"() {
        def ans = POST("/db/foo/q/bar/out/a?b", [:])

        expect:
        ans.code == 400
    }

    def "List outputs"() {
        def ans = GET("/db/foo/q/bar/out")

        expect:
        ans.code == 200
        ans.json.size() == 1
        ans.json[0].id == "rabbit"
        ans.json[0].oid.length() > 0
    }

    def "Count outputs"() {
        def ans = GET("/db/foo/q/bar/out?count=true")

        expect:
        ans.code == 200
        ans.json.count == 1
    }

    def "Get output"() {
        def ans = GET("/db/foo/q/bar/out/rabbit")

        expect:
        ans.code == 200
        ans.json.id == "rabbit"
        ans.json.oid.length() > 0
    }

    def "Update output"() {
        def ans = PUT("/db/foo/q/bar/out/rabbit", [enabled: false])

        expect:
        ans.code == 200
        ans.json.id == "rabbit"
        ans.json.oid.length() > 0
        ans.json.enabled == false
    }

    def "Output type validation"() {
        def ans = POST("/db/foo/q/bar/out/piggy", [type: "oinks"])

        expect:
        ans.code == 400
    }

    def "Delete output"() {
        def ans = DELETE("/db/foo/q/bar/out/rabbit")

        expect:
        ans.code == 200
    }
}
