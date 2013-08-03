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
class InputsSpec extends StandaloneBase {

    def setupSpec() {
        assert POST("/db/foo", [:]).code == 201
        assert POST("/db/foo/q/bar", [maxSize: 1000000]).code == 201
    }

    def "Create input"() {
        def data = [type: "rabbitmq", url: "amqp://127.0.0.1/", enabled: false]
        def ans = POST("/db/foo/q/bar/in/rabbit", data)
        def ans2 = POST("/db/foo/q/bar/in/rabbit", data)

        expect:
        ans.code == 201
        ans.json.id == "rabbit"
        ans.json.type == data.type
        ans.json.url == data.url
        ans.json.enabled == false
        ans2.code == 200
    }

    def "Input id validation"() {
        def ans = POST("/db/foo/q/bar/in/a?b", [:])

        expect:
        ans.code == 400
    }

    def "List inputs"() {
        def ans = GET("/db/foo/q/bar/in")

        expect:
        ans.code == 200
        ans.json.size() == 1
        ans.json[0].id == "rabbit"
    }

    def "Count inputs"() {
        def ans = GET("/db/foo/q/bar/in?count=true")

        expect:
        ans.code == 200
        ans.json.count == 1
    }

    def "Get input"() {
        def ans = GET("/db/foo/q/bar/in/rabbit")

        expect:
        ans.code == 200
        ans.json.id == "rabbit"
    }

    def "Update input"() {
        def ans = PUT("/db/foo/q/bar/in/rabbit", [enabled: false])

        expect:
        ans.code == 200
        ans.json.id == "rabbit"
        ans.json.enabled == false
    }

    def "Input type validation"() {
        def ans = POST("/db/foo/q/bar/in/piggy", [type: "oinks"])

        expect:
        ans.code == 400
    }

    def "Delete input"() {
        def ans = DELETE("/db/foo/q/bar/in/rabbit")

        expect:
        ans.code == 200
    }
}
