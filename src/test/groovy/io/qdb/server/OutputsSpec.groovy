package io.qdb.server

import spock.lang.Stepwise

@Stepwise
class OutputsSpec extends StandaloneBase {

    def setupSpec() {
        assert POST("/databases/foo", [:]).code == 201
        assert POST("/databases/foo/queues/bar", [maxSize: 1000000]).code == 201
    }

    def "Create output"() {
        def data = [type: "rabbitmq", url: "amqp://127.0.0.1/"]
        def ans = POST("/databases/foo/queues/bar/outputs/rabbit", data)
        def ans2 = POST("/databases/foo/queues/bar/outputs/rabbit", data)

        expect:
        ans.code == 201
        ans.json.id == "rabbit"
        ans.json.oid.length() > 0
        ans.json.type == data.type
        ans.json.url == data.url
        ans.json.enabled == true
        ans2.code == 200
    }

    def "Output id validation"() {
        def ans = POST("/databases/foo/queues/bar/outputs/a?b", [:])

        expect:
        ans.code == 400
    }

    def "List outputs"() {
        def ans = GET("/databases/foo/queues/bar/outputs")

        expect:
        ans.code == 200
        ans.json.size() == 1
        ans.json[0].id == "rabbit"
        ans.json[0].oid.length() > 0
    }

    def "Count outputs"() {
        def ans = GET("/databases/foo/queues/bar/outputs?count=true")

        expect:
        ans.code == 200
        ans.json.count == 1
    }

    def "Get output"() {
        def ans = GET("/databases/foo/queues/bar/outputs/rabbit")

        expect:
        ans.code == 200
        ans.json.id == "rabbit"
        ans.json.oid.length() > 0
    }

    def "Update output"() {
        def ans = PUT("/databases/foo/queues/bar/outputs/rabbit", [enabled: false])

        expect:
        ans.code == 200
        ans.json.id == "rabbit"
        ans.json.oid.length() > 0
        ans.json.enabled == false
    }

    def "Output type validation"() {
        def ans = POST("/databases/foo/queues/bar/outputs/piggy", [type: "oinks"])

        expect:
        ans.code == 400
    }

    def "Delete output"() {
        def ans = DELETE("/databases/foo/queues/bar/outputs/rabbit")

        expect:
        ans.code == 200
    }
}
