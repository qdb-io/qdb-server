package io.qdb.server

import spock.lang.Stepwise

@Stepwise
class MessagesSpec extends Base {

    def setupSpec() {
        POST("/users", [id: "david", password: "secret"])
        POST("/databases", [id: "foo", owner: "david"])
        POST("/databases/foo/queues", [id: "bar", maxSize: 10000000], "david", "secret")
    }

    def "Append message with contentLength header"() {
        long now = System.currentTimeMillis()
        def ans = POST("/databases/foo/queues/bar/messages", [hello: "world"], "david", "secret")
        def ans2 = POST("/databases/foo/queues/bar/messages", [hello: "2nd world"], "david", "secret")

        expect:
        ans.id == 0
        ans.timestamp >= now
        ans.timestamp < now + 30 * 1000L
        ans2.id > ans.id
        ans2.timestamp >= ans.timestamp
        ans2.timestamp < now + 30 * 1000L
    }

}
