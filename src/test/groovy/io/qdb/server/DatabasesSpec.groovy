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
class DatabasesSpec extends StandaloneBase {

    def setupSpec() {
        assert POST("/users/david", [password: "secret"]).code == 201
        assert POST("/users/gimp", [password: "secret"]).code == 201
    }

    def "Create database"() {
        def ans = POST("/db/foo", [:])
        def ans2 = POST("/db/foo", [:])

        expect:
        ans.code == 201
        ans.json.id == "foo"
        ans2.code == 200
    }

    def "List databases for admin"() {
        def ans = GET("/db")

        expect:
        ans.code == 200
        ans.json.size() == 2
        ans.json[0].id == "default"
        ans.json[1].id == "foo"
    }

    def "Count databases"() {
        def ans = GET("/db?count=true")

        expect:
        ans.code == 200
        ans.json.count == 2
    }

    def "Get database"() {
        def ans = GET("/db/foo")

        expect:
        ans.code == 200
        ans.json.id == "foo"
    }

    def "Update database"() {
        def ans = PUT("/db/foo", [owner: "david"])

        expect:
        ans.code == 200
        ans.json.id == "foo"
        ans.json.owner == "david"
    }

    def "Get database as owner"() {
        def ans = GET("/db/foo", "david", "secret")

        expect:
        ans.code == 200
        ans.json.id == "foo"
    }

    def "Get database as arb user"() {
        def ans = GET("/db/foo", "gimp", "secret")

        expect:
        ans.code == 403
    }

    def "Bogus resource gives 404"() {
        def ans = GET("/db/foo/wibble")

        expect:
        ans.code == 404
    }

    def "Delete database"() {
        def ans = DELETE("/db/foo")
        def ans2 = GET("/db/foo")

        expect:
        ans.code == 200
        ans2.code == 404
    }

    def "Delete database with queues"() {
        assert POST("/db/foo", [:]).code == 201
        assert POST("/db/foo/q/bar", [maxSize: 1000000]).code == 201
        def ans = DELETE("/db/foo")
        def ans2 = GET("/db/foo")

        expect:
        ans.code == 200
        ans2.code == 404
    }

}
