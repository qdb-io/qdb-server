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

import io.qdb.server.databind.DateTimeParser
import spock.lang.Specification

import java.text.SimpleDateFormat

class DateTimeParserSpec extends Specification {

    def "Millis works"() {
        Date d = DateTimeParser.INSTANCE.parse("2013-06-16T21:04:32.123+0300")
        def df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        df.setTimeZone(TimeZone.getTimeZone("GMT+3"))

        expect:
        df.format(d) == "2013-06-16T21:04:32.123+0300"
    }

    def "Millis no timezone works"() {
        Date d = DateTimeParser.INSTANCE.parse("2013-06-16T21:04:32.123")
        def df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")

        expect:
        df.format(d) == "2013-06-16T21:04:32.123"
    }

    def "Full works"() {
        Date d = DateTimeParser.INSTANCE.parse("2013-06-16T21:04:32+0300")
        def df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
        df.setTimeZone(TimeZone.getTimeZone("GMT+3"))

        expect:
        df.format(d) == "2013-06-16T21:04:32+0300"
    }

    def "Full no timezone works"() {
        Date d = DateTimeParser.INSTANCE.parse("2013-06-16T21:04:32")
        def df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

        expect:
        df.format(d) == "2013-06-16T21:04:32"
    }

    def "Full no secs works"() {
        Date d = DateTimeParser.INSTANCE.parse("2013-06-16T21:04")
        def df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

        expect:
        df.format(d) == "2013-06-16T21:04:00"
    }

    def "Date only works"() {
        Date d = DateTimeParser.INSTANCE.parse("2013-06-16")
        def df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        expect:
        df.format(d) == "2013-06-16 00:00:00"
    }

    def "Time only with seconds works"() {
        Date d = DateTimeParser.INSTANCE.parse("21:04:32")
        def df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        String now = df.format(new Date()).substring(0, 10)

        expect:
        df.format(d) == now + " 21:04:32"
    }

    def "Time only without seconds works"() {
        Date d = DateTimeParser.INSTANCE.parse("21:04")
        def df = new SimpleDateFormat("yyyy-MM-dd HH:mm")
        String now = df.format(new Date()).substring(0, 10)

        expect:
        df.format(d) == now + " 21:04"
    }

    def "Format timestamp works"() {
        Date d = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").parse("2013-06-16T21:04:32.123+0200")
        String s = DateTimeParser.INSTANCE.formatTimestamp(d)

        expect:
        s == "2013-06-16T21:04:32.123+0200"
    }
}
