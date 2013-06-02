package io.qdb.server

import spock.lang.Stepwise

@Stepwise
class DatabasesSpec extends StandaloneBase {

    def setupSpec() {
        assert POST("/users/david", [password: "secret"]).code == 201
        assert POST("/users/gimp", [password: "secret"]).code == 201
    }

    def "Create database"() {
        def ans = POST("/databases/foo", [:])
        def ans2 = POST("/databases/foo", [:])

        expect:
        ans.code == 201
        ans.json.id == "foo"
        ans2.code == 200
    }

    def "List databases for admin"() {
        def ans = GET("/databases")

        expect:
        ans.code == 200
        ans.json.size() == 1
        ans.json[0].id == "foo"
    }

    def "Count databases"() {
        def ans = GET("/databases?count=true")

        expect:
        ans.code == 200
        ans.json.count == 1
    }

    def "Get database"() {
        def ans = GET("/databases/foo")

        expect:
        ans.code == 200
        ans.json.id == "foo"
    }

    def "Update database"() {
        def ans = PUT("/databases/foo", [owner: "david"])

        expect:
        ans.code == 200
        ans.json.id == "foo"
        ans.json.owner == "david"
    }

    def "Get database as owner"() {
        def ans = GET("/databases/foo", "david", "secret")

        expect:
        ans.code == 200
        ans.json.id == "foo"
    }

    def "Get database as arb user"() {
        def ans = GET("/databases/foo", "gimp", "secret")

        expect:
        ans.code == 403
    }

    def "Bogus resource gives 404"() {
        def ans = GET("/databases/foo/wibble")

        expect:
        ans.code == 404
    }

//    def "Delete database"() {
//        def ans = DELETE("/databases/foo")
//        def ans2 = GET("/databases/foo")
//
//        expect:
//        ans.code == 200
//        ans2.code == 404
//    }

}
