package io.qdb.server

import spock.lang.Stepwise

@Stepwise
class MessagesSpec extends Base {

    def setupSpec() {
        assert POST("/users", [id: "david", password: "secret"]).code == 201
        assert POST("/databases", [id: "foo", owner: "david"]).code == 201
        assert POST("/databases/foo/queues", [id: "bar", maxSize: 100000000], "david", "secret").code == 201
    }

    def "Append message"() {
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
        println(ans.text)

        expect:
        ans.code == 201
        ans.json.id > 0
        ans.json.payloadSize == 30000
    }
}
