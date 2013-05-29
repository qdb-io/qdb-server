package io.qdb.server

import spock.lang.Stepwise
import spock.lang.Shared

@Stepwise
class QueuesSpec extends StandaloneBase {

    def setupSpec() {
        assert POST("/users/david", [password: "secret"]).code == 201
        assert POST("/db/foo", [owner: "david"]).code == 201
    }

    def "Create queue"() {
        def data = [maxSize: 10000000]
        def ans = POST("/db/foo/queues/bar", data, "david", "secret")
        println(ans)
        def ans2 = POST("/db/foo/queues/bar", data, "david", "secret")

        expect:
        ans.code == 201
        ans.json.id == "bar"
        ans.json.qid.length() > 0
        ans.json.maxSize == 10000000
        ans.json.maxPayloadSize == 128 * 1024
        ans.json.contentType == "application/json; charset=utf-8"
        ans2.code == 200
    }

    def "Queue id validation"() {
        def ans = POST("/db/foo/queues/a?b", [:], "david", "secret")

        expect:
        ans.code == 400
    }

    def "List queues"() {
        def ans = GET("/db/foo/queues", "david", "secret")

        expect:
        ans.code == 200
        ans.json.size() == 1
        ans.json[0].id == "bar"
        ans.json[0].qid.length() > 0
    }

    def "Count queues"() {
        def ans = GET("/db/foo/queues?count=true", "david", "secret")

        expect:
        ans.code == 200
        ans.json.count == 1
    }

    def "Get queue"() {
        def ans = GET("/db/foo/queues/bar", "david", "secret")

        expect:
        ans.code == 200
        ans.json.id == "bar"
        ans.json.qid.length() > 0
    }

    def "Update queue"() {
        def ans = PUT("/db/foo/queues/bar", [maxSize: 20000000, maxPayloadSize: 100000], "david", "secret")

        expect:
        ans.code == 200
        ans.json.id == "bar"
        ans.json.qid.length() > 0
        ans.json.maxSize == 20000000
        ans.json.maxPayloadSize == 100000
    }

    def "Queue maxSize validation"() {
        def ans = PUT("/db/foo/queues/bar", [maxSize: 100000], "david", "secret")

        expect:
        ans.code == 400
    }

    def "Queue maxPayloadSize validation"() {
        def ans = PUT("/db/foo/queues/bar", [maxPayloadSize: 20000000 / 3 + 1], "david", "secret")

        expect:
        ans.code == 400
    }
}
