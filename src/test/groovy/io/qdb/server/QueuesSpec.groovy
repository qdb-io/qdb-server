package io.qdb.server

import spock.lang.Stepwise
import spock.lang.Shared

@Stepwise
class QueuesSpec extends StandaloneBase {

    def setupSpec() {
        assert POST("/users/david", [password: "secret"]).code == 201
        assert POST("/databases/foo", [owner: "david"]).code == 201
    }

    def "Create queue"() {
        def data = [maxSize: "10m"]
        def ans = POST("/databases/foo/queues/bar", data, true, "david", "secret")
        def ans2 = POST("/databases/foo/queues/bar", data, "david", "secret")

        expect:
        ans.code == 201
        ans.json.id == "bar"
        ans.json.qid.length() > 0
        ans.json.maxSize == 10 * 1024 * 1024
        ans.json.maxPayloadSize == 1024 * 1024
        ans.json.contentType == "application/json; charset=utf-8"
        ans2.code == 200
    }

    def "Queue id validation"() {
        def ans = POST("/databases/foo/queues/a@b", [:], "david", "secret")

        expect:
        ans.code == 400
    }

    def "List queues"() {
        def ans = GET("/databases/foo/queues", "david", "secret")

        expect:
        ans.code == 200
        ans.json.size() == 1
        ans.json[0].id == "bar"
        ans.json[0].qid.length() > 0
    }

    def "Count queues"() {
        def ans = GET("/databases/foo/queues?count=true", "david", "secret")

        expect:
        ans.code == 200
        ans.json.count == 1
    }

    def "Get queue"() {
        def ans = GET("/databases/foo/queues/bar", "david", "secret")

        expect:
        ans.code == 200
        ans.json.id == "bar"
        ans.json.qid.length() > 0
    }

    def "Update queue"() {
        def ans = PUT("/databases/foo/queues/bar", [maxSize: 20000000, maxPayloadSize: 100000], "david", "secret")

        expect:
        ans.code == 200
        ans.json.id == "bar"
        ans.json.qid.length() > 0
        ans.json.maxSize == 20000000
        ans.json.maxPayloadSize == 100000
    }

    def "Queue maxSize validation"() {
        def ans = PUT("/databases/foo/queues/bar", [maxSize: 100000], "david", "secret")

        expect:
        ans.code == 400
    }

    def "Queue maxPayloadSize validation"() {
        def ans = PUT("/databases/foo/queues/bar", [maxPayloadSize: 20000000 / 3 + 1], "david", "secret")

        expect:
        ans.code == 400
    }

    def "Delete queue"() {
        def ans = DELETE("/databases/foo/queues/bar", "david", "secret")
        def ans2 = GET("/databases/foo/queues/bar")

        expect:
        ans.code == 200
        ans2.code == 404
    }

    def "Delete queue with outputs"() {
        assert POST("/databases/foo/queues/bar", [maxSize: 1000000]).code == 201
        assert POST("/databases/foo/queues/bar/outputs/rabbit", [type: "rabbitmq"]).code == 201
        def ans = DELETE("/databases/foo/queues/bar")
        def ans2 = GET("/databases/foo/queues/bar/outputs/rabbit")
        def ans3 = GET("/databases/foo/queues/bar")

        expect:
        ans.code == 200
        ans2.code == 404
        ans3.code == 404
    }
}
