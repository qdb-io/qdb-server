package io.qdb.server

class DatabasesSpec extends BaseSpec {

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

    def "List databases for non-admin user"() {
        def ans = GET("/databases", "david", "secret")

        expect:
        ans.size() == 1
        ans[0].id == "foo"
    }

}
