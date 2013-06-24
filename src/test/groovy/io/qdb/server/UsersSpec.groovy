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
class UsersSpec extends StandaloneBase {

    def setupSpec() {
        assert POST("/db/foo", [:]).code == 201
    }

    def "List users"() {
        def ans = GET("/users")

        expect:
        ans.code == 200
        ans.json.size() == 1
        ans.json[0].id == "admin"
        ans.json[0].passwordHash == null
        ans.json[0].admin == true
    }

    def "Count users"() {
        def ans = GET("/users?count=true")

        expect:
        ans.code == 200
        ans.json.count == 1
    }

    def "Get user"() {
        def ans = GET("/users/admin")

        expect:
        ans.code == 200
        ans.json.id == "admin"
        ans.json.admin == true
    }

    def "Create user"() {
        def map = [password: "secret", admin: false, databases: ["foo"]]
        def ans = POST("/users/david", map)
        def ans2 = POST("/users/david", map)

        expect:
        ans.code == 201
        ans.json.id == "david"
        ans.json.passwordHash == null
        ans.json.admin == false
        ans.json.databases == ["foo"]
        ans2.code == 200
    }

    def "Update user"() {
        def ver = GET("/users/david").json.version
        def a = PUT("/users/david", [admin: true, version: ver])
        def b = PUT("/users/david", [admin: false, version: ver + 1])

        expect:
        a.code == 200
        a.json.id == "david"
        a.json.admin == true
        a.json.version == ver + 1
        b.code == 200
        b.json.admin == false
    }

    def "Update user with old version"() {
        def ans = PUT("/users/david", [admin: true, version: 0])

        expect:
        ans.code == 409
        ans.json.id == "david"
    }

    def "List databases for non-admin user"() {
        def ans = GET("/db", "david", "secret")

        expect:
        ans.code == 200
        ans.json.size() == 1
        ans.json[0].id == "foo"
    }

    def "Delete user"() {
        def ans = DELETE("/users/david")

        expect:
        ans.code == 200
    }

}
