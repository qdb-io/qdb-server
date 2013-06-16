package io.qdb.server

import io.qdb.server.databind.DateTimeParser
import spock.lang.Specification

import java.text.SimpleDateFormat

class DateTimeParserSpec extends Specification {

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
}
