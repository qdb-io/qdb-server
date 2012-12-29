package io.qdb.server

import spock.lang.Stepwise

import spock.lang.Shared
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import groovy.json.JsonSlurper

@Stepwise
class MessagesSpec extends Base {

    @Shared ExecutorService pool = Executors.newCachedThreadPool()
    @Shared long startTime = System.currentTimeMillis()

    def setupSpec() {
        assert POST("/users", [id: "david", password: "secret"]).code == 201
        assert POST("/databases", [id: "foo", owner: "david"]).code == 201
        assert POST("/databases/foo/queues", [id: "bar", maxSize: 10000000], "david", "secret").code == 201
    }

    def cleanupSpec() {
        pool.shutdownNow()
    }

    def "Append message"() {
        long now = System.currentTimeMillis()
        def ans = POST("/databases/foo/queues/bar/messages?routingKey=abc", [hello: "world"], "david", "secret")
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

    def "Append message with chunked transfer encoding"() {
        def url = new URL(client.serverUrl + "/databases/foo/queues/bar/messages")
        HttpURLConnection con = url.openConnection() as HttpURLConnection
        con.doOutput = true
        con.requestMethod = "POST"
        con.setRequestProperty("Authorization", client.toBasicAuth("david", "secret"))
        con.setRequestProperty("Content-Type", "application/json")
        con.setChunkedStreamingMode(1024)
        con.outputStream.write(new byte[30000])
        Client.Response ans = new Client.Response(con)

        expect:
        ans.code == 201
        ans.json.id > 0
        ans.json.payloadSize == 30000
    }

    def "Get single message"() {
        def ans = GET("/databases/foo/queues/bar/messages?id=0&single=true")

        expect:
        ans.code == 200
        ans.json.hello == "world"
        ans.headers["X-QDB-Id"] == "0"
        ans.headers["X-QBD-Timestamp"] > startTime.toString()
        ans.headers["X-QDB-RoutingKey"] == "abc"
        ans.headers["Content-Type"] == "application/json; charset=utf-8"
    }

    def "Get 2 messages streamed"() {
        def ans = GET("/databases/foo/queues/bar/messages?id=0&limit=2")
        println(ans.text)
        def r = new StringReader(ans.text)

        def h1 = new JsonSlurper().parseText(r.readLine())
        def m1line = r.readLine()
        def m1 = new JsonSlurper().parseText(m1line)

        def h2 = new JsonSlurper().parseText(r.readLine())
        def m2line = r.readLine()
        def m2 = new JsonSlurper().parseText(m2line)

        expect:
        ans.code == 200

        h1.id == 0
        h1.timestamp >= startTime
        h1.payloadSize == m1line.length()
        h1.routingKey == "abc"
        m1.hello == "world"

        h2.id > 0
        h2.timestamp >= h1.startTime
        h2.payloadSize == m2line.length()
        h2.routingKey == ""
        m2.hello == "2nd world"
    }
}
