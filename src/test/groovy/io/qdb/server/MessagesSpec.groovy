package io.qdb.server

import spock.lang.Stepwise

@Stepwise
class MessagesSpec extends Base {

    def setupSpec() {
        assert POST("/users", [id: "david", password: "secret"]).code == 201
        assert POST("/databases", [id: "foo", owner: "david"]).code == 201
        assert POST("/databases/foo/queues", [id: "bar", maxSize: 10000000], "david", "secret").code == 201
    }

    def "Append message with ContentLength header"() {
        long now = System.currentTimeMillis()
        def ans = POST("/databases/foo/queues/bar/messages", [hello: "world"], "david", "secret")
        def ans2 = POST("/databases/foo/queues/bar/messages", [hello: "2nd world"], "david", "secret")

        expect:
        ans.code == 201
        ans.json.id == 0
        ans.json.timestamp >= now
        ans.json.timestamp < now + 30 * 1000L
        ans2.code == 201
        ans2.json.id > ans.json.id
        ans2.json.timestamp >= ans.json.timestamp
        ans2.json.timestamp < now + 30 * 1000L
    }

//    def "Append message with no ContentLength header"() {
//
//    }
}
