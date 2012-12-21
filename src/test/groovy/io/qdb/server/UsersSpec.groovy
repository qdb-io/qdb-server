package io.qdb.server

import spock.lang.Stepwise

@Stepwise
class UsersSpec extends Base {

    def setupSpec() {
        POST("/databases", [id: "foo"])
    }

    def "List users"() {
        def ans = GET("/users")

        expect:
        ans.size() == 1
        ans[0].id == "admin"
        ans[0].passwordHash == null
        ans[0].admin == true
    }

    def "Count users"() {
        def ans = GET("/users?count=true")

        expect:
        ans.count == 1
    }

    def "Get user"() {
        def ans = GET("/users/admin")

        expect:
        ans.id == "admin"
        ans.admin == true
    }

    def "Create user"() {
        def ans = POST("/users", [id: "david", password: "secret", admin: false, databases: ["foo"]])

        expect:
        ans.id == "david"
        ans.passwordHash == null
        ans.admin == false
        ans.databases == ["foo"]
    }

    def "Update user"() {
        def a = PUT("/users/david", [admin: true])
        def b = PUT("/users/david", [admin: false])

        expect:
        a.id == "david"
        a.admin == true
        b.admin == false
    }

    def "Update user with correct version"() {
        def ver = GET("/users/david").version
        def a = PUT("/users/david", [admin: true, version: ver])
        def b = PUT("/users/david", [admin: false, version: ver + 1])

        expect:
        a.id == "david"
        a.admin == true
        a.version == ver + 1
        b.admin == false
    }

    def "Update user with old version"() {
        when:
        PUT("/users/david", [admin: true, version: 0])

        then:
        BadResponseCodeException e = thrown()
        e.responseCode == 409
        e.json.id == "david"
    }

    def "List databases for non-admin user"() {
        def ans = GET("/databases", "david", "secret")

        expect:
        ans.size() == 1
        ans[0].id == "foo"
    }

}
