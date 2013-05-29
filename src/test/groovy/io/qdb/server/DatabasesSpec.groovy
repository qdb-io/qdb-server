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
        ans.json.size() == 1
        ans.json[0].id == "foo"
    }

    def "Count databases"() {
        def ans = GET("/db?count=true")

        expect:
        ans.code == 200
        ans.json.count == 1
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

}
