package io.qdb.server

import spock.lang.Stepwise

@Stepwise
class DatabasesSpec extends Base {

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

}
