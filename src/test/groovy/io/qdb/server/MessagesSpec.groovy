/*
 * Copyright 2013 David Tinker
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.qdb.server

import spock.lang.Stepwise

import spock.lang.Shared

import java.text.SimpleDateFormat
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import groovy.json.JsonSlurper
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@Stepwise
class MessagesSpec extends StandaloneBase {

    @Shared ExecutorService pool = Executors.newCachedThreadPool()
    @Shared long startTime = System.currentTimeMillis()

    def setupSpec() {
        assert POST("/users/david", [password: "secret"]).code == 201
        assert POST("/db/foo", [owner: "david"]).code == 201
        assert POST("/db/foo/q/bar", [maxSize: 10000000, maxPayloadSize: 30000], "david", "secret").code == 201
        assert POST("/db/foo/q/lots", [maxSize: 10000000, maxPayloadSize: 5000], "david", "secret").code == 201
    }

    def cleanupSpec() {
        pool.shutdownNow()
    }

    def "Append message"() {
        long now = System.currentTimeMillis()
        now = now - now % 1000
        def ans = POST("/db/foo/q/bar/messages?routingKey=abc", [hello: "world"], "david", "secret")
        def ans2 = POST("/db/foo/q/bar/messages", [hello: "2nd world"], "david", "secret")
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        long ts = df.parse(ans.json.timestamp).time
        long ts2 = df.parse(ans2.json.timestamp).time

        expect:
        ans.code == 201
        ans.json.id == 0
        ts >= now
        ts < now + 30 * 1000L
        ans2.code == 201
        ans2.json.id > ans.json.id
        ts2 >= ts
        ts2 < now + 30 * 1000L
    }

    def "Append message with chunked transfer encoding"() {
        def con = openConTo("/db/foo/q/bar/messages")
        con.outputStream.write(new byte[30000])
        Client.Response ans = new Client.Response(con)

        expect:
        ans.code == 201
        ans.json.id > 0
        ans.json.payloadSize == 30000
    }

    def "Append message with payload too long"() {
        def con = openConTo("/db/foo/q/bar/messages?routingKey=abc")
        con.outputStream.write(new byte[30001])
        Client.Response ans = new Client.Response(con)

        expect:
        ans.code == 422
    }

    def "Get single message"() {
        def ans = GET("/db/foo/q/bar/messages?id=0&single=true")

        expect:
        ans.code == 200
        ans.json.hello == "world"
        ans.headers["X-QDB-Id"] == "0"
        ans.headers["X-QBD-Timestamp"] > startTime.toString()
        ans.headers["X-QDB-RoutingKey"] == "abc"
        ans.headers["Content-Type"] == "application/json; charset=utf-8"
    }

    def "Get 2 messages streamed"() {
        def ans = GET("/db/foo/q/bar/messages?id=0&limit=2")
        def r = new StringReader(ans.text)

        def h1 = new JsonSlurper().parseText(r.readLine())
        def m1line = r.readLine()
        def m1 = new JsonSlurper().parseText(m1line)

        def h2 = new JsonSlurper().parseText(r.readLine())
        def m2line = r.readLine()
        def m2 = new JsonSlurper().parseText(m2line)

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        expect:
        ans.code == 200

        h1.id == 0
        df.parse(h1.timestamp).time >= startTime
        h1.payloadSize == m1line.length()
        h1.routingKey == "abc"
        m1.hello == "world"

        h2.id > 0
        df.parse(h2.timestamp).time >= h1.startTime
        h2.payloadSize == m2line.length()
        h2.routingKey == ""
        m2.hello == "2nd world"
    }

    def "Get message by timestamp"() {
        def ans = GET("/db/foo/q/bar/messages?at=${startTime}&single=true")

        expect:
        ans.code == 200
        ans.json.hello == "world"
    }

    def "Wait for new message"() {
        CountDownLatch ready = new CountDownLatch(1)
        CountDownLatch done = new CountDownLatch(1)
        def ans = null
        pool.execute({
            ready.countDown()
            ans = GET("/db/foo/q/bar/messages?single=true")
            done.countDown()
        })
        ready.await(200, TimeUnit.MILLISECONDS)
        Thread.sleep(50)    // give background GET time to block waiting for message
        POST("/db/foo/q/bar/messages?routingKey=abc", [hello: "3rd world"])
        done.await(200, TimeUnit.MILLISECONDS)

        expect:
        ans.code == 200
        ans.json.hello == "3rd world"
    }

    def "Wait for new message with timeout"() {
        CountDownLatch ready = new CountDownLatch(1)
        CountDownLatch done = new CountDownLatch(1)
        def ans = null
        pool.execute({
            ready.countDown()
            ans = GET("/db/foo/q/bar/messages?single=true&timeoutMs=1000")
            done.countDown()
        })
        ready.await(200, TimeUnit.MILLISECONDS)
        Thread.sleep(50)    // give background GET time to block waiting for message
        POST("/db/foo/q/bar/messages", [hello: "4th world"])
        done.await(200, TimeUnit.MILLISECONDS)

        expect:
        ans.code == 200
        ans.json.hello == "4th world"
    }

    def "Wait for new message with timeout expiry"() {
        long now = System.currentTimeMillis()
        def ans = GET("/db/foo/q/bar/messages?timeoutMs=50")
        def ms = System.currentTimeMillis() - now

        expect:
        ans.code == 200
        ans.text == ""
        ms >= 50 && ms <= 200
    }

    def "Keep-alive chars sent with timeout"() {
        // time for one keep-alive \n to be sent
        def ans = GET("/db/foo/q/bar/messages?timeoutMs=75&keepAliveMs=50")

        expect:
        ans.code == 200
        ans.text == "\n"
    }

    def "Keep-alive chars sent"() {
        CountDownLatch ready = new CountDownLatch(1)
        CountDownLatch done = new CountDownLatch(1)
        def ans = null
        pool.execute({
            ready.countDown()
            ans = GET("/db/foo/q/bar/messages?keepAliveMs=50&limit=1")
            done.countDown()
        })
        ready.await(200, TimeUnit.MILLISECONDS)
        Thread.sleep(120)    // give background GET time to block waiting for message and 2 keep-alive chars sent
        assert POST("/db/foo/q/bar/messages", [hello: "5th world"]).code == 201
        done.await(200, TimeUnit.MILLISECONDS)

        expect:
        ans.code == 200
        ans.text.substring(0, 3) == "\n\n{"     // 2 keep-alive chars sent
    }

    def "GET?count=true gives 400"() {
        def ans = GET("/db/foo/q/bar/messages?count=true")

        expect:
        ans.code == 400
    }

    private HttpURLConnection openConTo(String endpoint) {
        def url = new URL(client.serverUrl + endpoint)
        HttpURLConnection con = url.openConnection() as HttpURLConnection
        con.doOutput = true
        con.requestMethod = "POST"
        con.setRequestProperty("Authorization", client.toBasicAuth("david", "secret"))
        con.setRequestProperty("Content-Type", "application/octet-stream")
        con.setChunkedStreamingMode(1024)
        return con
    }

    private HttpURLConnection openConToLots() {
        return openConTo("/db/foo/q/lots/messages?multiple=true")
    }

    def "Append multiple error checking"() {
        HttpURLConnection con = openConToLots()
        con.outputStream.write((int)'{'.charAt(0))
        def ans = new Client.Response(con)

        expect:
        ans.code == 422
    }

    def "Append multiple partial failure"() {
        HttpURLConnection con = openConToLots()
        con.outputStream.write("3:key\n1:x\n\nbad".getBytes("UTF8"))
        def ans = new Client.Response(con)

        expect:
        ans.code == 422
        ans.json.responseCode == 422
        ans.json.created.size() == 1
    }

    def "Append multiple empty returns 200"() {
        HttpURLConnection con = openConToLots()
        def ans = new Client.Response(con)

        expect:
        ans.code == 200
        ans.json.size() == 0
    }

    def "Append multiple missing payload fails"() {
        HttpURLConnection con = openConToLots()
        con.outputStream.write("3:key".getBytes("UTF8"))
        def ans = new Client.Response(con)

        expect:
        ans.code == 422
        ans.json.responseCode == 422
        ans.json.created == null
    }

    def "Append multiple payload too long fails"() {
        HttpURLConnection con = openConToLots()
        def out = con.outputStream
        out.write("3:key\n5001:".getBytes("UTF8"))
        out.write(new byte[5001])
        def ans = new Client.Response(con)

        expect:
        ans.code == 422
    }

    def "Append multiple length short fails"() {
        HttpURLConnection con = openConToLots()
        con.outputStream.write("3:ke".getBytes("UTF8"))
        def ans = new Client.Response(con)

        expect:
        ans.code == 422
    }

    def "Append multiple dodgy length fails"() {
        HttpURLConnection con = openConToLots()
        con.outputStream.write(((1L + Integer.MAX_VALUE) + ":").getBytes("UTF8"))
        def ans = new Client.Response(con)

        expect:
        ans.code == 422
    }

    def "Append multiple"() {
        HttpURLConnection con = openConToLots()
        def out = con.outputStream
        int n = 100
        byte[] buf = new byte[4096]
        def rnd = new Random(123)
        rnd.nextBytes(buf);
        def CRLF = '\r\n'.getBytes("UTF8")
        for (int i = 0; i < n; i++) {
            out.write(CRLF) // not required but must be ignored
            int sz = rnd.nextInt(buf.length)
            String routingKey = "key" + i
            out.write((routingKey.length() + ":" + routingKey).getBytes("UTF8"))
            out.write(CRLF) // not required but must be ignored
            out.write((sz + ":").getBytes("UTF8"))
            out.write(buf, 0, sz)
            out.flush()
        }
        def ans = new Client.Response(con)

        expect:
        ans.code == 201
        ans.json.size() == n
    }

}
