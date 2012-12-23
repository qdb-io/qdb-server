package io.qdb.server

import spock.lang.Stepwise

@Stepwise
class DatabasesSpec extends Base {

    def setupSpec() {
        POST("/users", [id: "david", password: "secret"])
        POST("/users", [id: "gimp", password: "secret"])
    }

    def "Create database"() {
        def ans = POST("/databases", [id: "foo"])

        expect:
        ans.id == "foo"
    }

    def "List databases for admin"() {
        def ans = GET("/databases")

        expect:
        ans.size() == 1
        ans[0].id == "foo"
    }

    def "Count databases"() {
        def ans = GET("/databases?count=true")

        expect:
        ans.count == 1
    }

    def "Get database"() {
        def ans = GET("/databases/foo")

        expect:
        ans.id == "foo"
    }

    def "Update database"() {
        def a = PUT("/databases/foo", [owner: "david"])

        expect:
        a.id == "foo"
        a.owner == "david"
    }

    def "Get database as owner"() {
        def ans = GET("/databases/foo", "david", "secret")

        expect:
        ans.id == "foo"
    }

    def "Get database as arb user"() {
        when:
        GET("/databases/foo", "gimp", "secret")

        then:
        BadResponseCodeException e = thrown()
        e.responseCode == 403
    }

}
