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

import java.text.SimpleDateFormat

@Stepwise
class TimelineSpec extends StandaloneBase {

    def setupSpec() {
        assert POST("/db/foo", [owner: "admin"]).code == 201
        assert POST("/db/foo/q/bar", [maxSize: 10000000]).code == 201
    }

    def "Get timeline for empty queue"() {
        def ans = GET("/db/foo/q/bar/timeline")

        expect:
        ans.code == 200
        ans.json.size() == 0
    }

    def "Get timeline"() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        long ts1 = df.parse(POST("/db/foo/q/bar/messages", [hello: "a"]).json.timestamp).time

        Thread.sleep(20);
        def json = POST("/db/foo/q/bar/messages", [hello: "b"]).json
        long id2 = json.id
        long ts2 = df.parse(json.timestamp).time

        def ans = GET("/db/foo/q/bar/timeline")

        expect:
        ans.code == 200
        ans.json.size() == 2

        ans.json[0].id == 1
        df.parse(ans.json[0].timestamp as String).time == ts1
        ans.json[0].count == 2
        ans.json[0].millis == ts2 - ts1
        ans.json[0].bytes == ans.json[1].id - 1

        ans.json[1].id == id2 + id2 - 1 /* size of a each message is also id2 */
        ans.json[1].count == 0
        ans.json[1].millis == 0
    }

    def "Get specific timeline"() {
        def ans = GET("/db/foo/q/bar/timeline/0")
        println ans.text

        expect:
        ans.code == 200
        ans.json.size() == 1
        ans.json[0].id == 1
        ans.json[0].count == 2
    }

    def "Get specific timeline with dodgy id"() {
        def ans = GET("/db/foo/q/bar/timeline/abc")

        expect:
        ans.code == 400
    }
}