package io.qdb.server

import spock.lang.Stepwise
import spock.lang.Shared

@Stepwise
class QueuesSpec extends Base {

    @Shared
    private String serverId;

    def setupSpec() {
        serverId = GET("/").id
        POST("/users", [id: "david", password: "secret"])
        POST("/databases", [id: "foo", owner: "david"])
    }

    def "Create queue"() {
        def ans = POST("/databases/foo/queues", [id: "bar", maxSize: 10000000], "david", "secret")

        expect:
        ans.id == "bar"
        ans.qid.length() > 0
        ans.master == serverId
        ans.maxSize == 10000000
    }

    def "Duplicate queue not allowed"() {
        when:
        POST("/databases/foo/queues", [id: "bar"], "david", "secret")

        then:
        BadResponseCodeException e = thrown()
        e.responseCode == 400
    }

    def "Queue id validation"() {
        when:
        POST("/databases/foo/queues", [id: "a?b"], "david", "secret")

        then:
        BadResponseCodeException e = thrown()
        e.responseCode == 400
    }

    def "List queues"() {
        def ans = GET("/databases/foo/queues", "david", "secret")

        expect:
        ans.size() == 1
        ans[0].id == "bar"
        ans[0].qid.length() > 0
        ans[0].master == serverId
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
        ans.master == serverId
    }

    def "Update queue"() {
        def ans = PUT("/databases/foo/queues/bar", [maxSize: 20000000, maxPayloadSize: 100000], "david", "secret")

        expect:
        ans.id == "bar"
        ans.qid.length() > 0
        ans.maxSize == 20000000
        ans.maxPayloadSize == 100000
    }

    def "Queue maxSize validation"() {
        when:
        PUT("/databases/foo/queues/bar", [maxSize: 100000], "david", "secret")

        then:
        BadResponseCodeException e = thrown()
        e.responseCode == 400
    }

    def "Queue maxPayloadSize validation"() {
        when:
        PUT("/databases/foo/queues/bar", [maxPayloadSize: 20000000 / 3 + 1], "david", "secret")

        then:
        BadResponseCodeException e = thrown()
        e.responseCode == 400
    }
}
