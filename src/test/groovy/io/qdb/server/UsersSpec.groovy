package io.qdb.server

import spock.lang.Stepwise

@Stepwise
class UsersSpec extends StandaloneBase {

    def setupSpec() {
        assert POST("/databases", [id: "foo"]).code == 201
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
        def ans = POST("/users", [id: "david", password: "secret", admin: false, databases: ["foo"]])

        expect:
        ans.code == 201
        ans.json.id == "david"
        ans.json.passwordHash == null
        ans.json.admin == false
        ans.json.databases == ["foo"]
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
        def ans = GET("/databases", "david", "secret")

        expect:
        ans.code == 200
        ans.json.size() == 1
        ans.json[0].id == "foo"
    }

}
