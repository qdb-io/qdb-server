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

import io.qdb.server.databind.DurationParser
import spock.lang.Specification

class DurationParserSpec extends Specification {

    def "Parse works"() {
        expect:
        DurationParser.parse("") == 0
        DurationParser.parse("3") == 3
        DurationParser.parse("30") == 30
        DurationParser.parse("1:30") == 90
        DurationParser.parse("12:30") == 12 * 60 + 30
        DurationParser.parse("4:12:30") == (4 * 60 + 12) * 60 + 30
        DurationParser.parse("14:12:30") == (14 * 60 + 12) * 60 + 30
        DurationParser.parse("2d") == 2 * 24 * 60 * 60
        DurationParser.parse("2D") == 2 * 24 * 60 * 60
        DurationParser.parse("2 d") == 2 * 24 * 60 * 60
        DurationParser.parse("2 days") == 2 * 24 * 60 * 60
        DurationParser.parse("2 days 1:30") == 2 * 24 * 60 * 60 + 90
        DurationParser.parse("2d 1:30") == 2 * 24 * 60 * 60 + 90
        DurationParser.parse("2d1:30") == 2 * 24 * 60 * 60 + 90
    }

    def "Parse throws IAE"() {
        when:
        DurationParser.parse("x")

        then:
        thrown(IllegalArgumentException)
    }
}
