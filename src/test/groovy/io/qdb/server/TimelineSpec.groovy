package io.qdb.server

import spock.lang.Stepwise

@Stepwise
class TimelineSpec extends Base {

    def setupSpec() {
        assert POST("/databases", [id: "foo", owner: "admin"]).code == 201
        assert POST("/databases/foo/queues", [id: "bar", maxSize: 10000000]).code == 201
    }

    def "Get timeline for empty queue"() {
        def ans = GET("/databases/foo/queues/bar/timeline")

        expect:
        ans.code == 200
        ans.json.size() == 0
    }

    def "Get timeline"() {
        long ts1 = POST("/databases/foo/queues/bar/messages", [hello: "a"]).json.timestamp

        Thread.sleep(20);
        def json = POST("/databases/foo/queues/bar/messages", [hello: "b"]).json
        long id2 = json.id
        long ts2 = json.timestamp

        def ans = GET("/databases/foo/queues/bar/timeline")

        expect:
        ans.code == 200
        ans.json.size() == 2

        ans.json[0].messageId == 0
        ans.json[0].timestamp == ts1
        ans.json[0].count == 2
        ans.json[0].millis == ts2 - ts1
        ans.json[0].bytes == ans.json[1].messageId

        ans.json[1].messageId == id2 + id2 /* size of a each message is also id2 */
        ans.json[1].count == 0
        ans.json[1].millis == 0
    }

    def "Get specific timeline"() {
        def ans = GET("/databases/foo/queues/bar/timeline/0")
        println ans.text

        expect:
        ans.code == 200
        ans.json.size() == 1
        ans.json[0].messageId == 0
        ans.json[0].count == 2
    }

    def "Get specific timeline with dodgy id"() {
        def ans = GET("/databases/foo/queues/bar/timeline/abc")

        expect:
        ans.code == 400
    }
}