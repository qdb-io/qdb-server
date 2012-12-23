package io.qdb.server

import spock.lang.Stepwise

@Stepwise
class QueuesSpec extends Base {

    def setupSpec() {
        POST("/users", [id: "david", password: "secret"])
        POST("/databases", [id: "foo", owner: "david"])
    }

    def "Create queue"() {
        def ans = POST("/databases/foo/queues", [id: "bar"], "david", "secret")

        expect:
        ans.id == "bar"
        ans.qid.length() > 0
    }

    def "List queues"() {
        def ans = GET("/databases/foo/queues", "david", "secret")

        expect:
        ans.size() == 1
        ans[0].id == "bar"
        ans[0].qid.length() > 0
    }

    def "Count queues"() {
        def ans = GET("/databases/foo/queues?count=true", "david", "secret")

        expect:
        ans.count == 1
    }

    def "Get queue"() {
        def ans = GET("/databases/foo/queues/bar", "david", "secret")

        expect:
        ans.id == "bar"
        ans.qid.length() > 0
    }
}
