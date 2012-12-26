package io.qdb.server

import spock.lang.Stepwise
import spock.lang.Shared

@Stepwise
class QueuesSpec extends Base {

    @Shared
    private String serverId

    def setupSpec() {
        serverId = GET("/").json.id
        assert POST("/users", [id: "david", password: "secret"]).code == 201
        assert POST("/databases", [id: "foo", owner: "david"]).code == 201
    }

    def "Create queue"() {
        def ans = POST("/databases/foo/queues", [id: "bar", maxSize: 10000000], "david", "secret")

        expect:
        ans.code == 201
        ans.json.id == "bar"
        ans.json.qid.length() > 0
        ans.json.master == serverId
        ans.json.maxSize == 10000000
        ans.json.maxPayloadSize == 128 * 1024
        ans.json.contentType == "application/json"
    }

    def "Duplicate queue not allowed"() {
        def ans = POST("/databases/foo/queues", [id: "bar"], "david", "secret")

        expect:
        ans.code == 400
    }

    def "Queue id validation"() {
        def ans = POST("/databases/foo/queues", [id: "a?b"], "david", "secret")

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
        ans.json[0].master == serverId
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
        ans.json.master == serverId
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
}
