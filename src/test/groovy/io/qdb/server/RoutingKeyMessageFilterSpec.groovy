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

import io.qdb.server.filter.MessageFilter
import io.qdb.server.filter.RoutingKeyMessageFilter
import spock.lang.Specification

class RoutingKeyMessageFilterSpec extends Specification {

    RoutingKeyMessageFilter f = new RoutingKeyMessageFilter()

    def "Regex"() {
        f.routingKey = "/[a-z]+"
        f.init(null)

        expect:
        f.accept(0, "abc", null) == MessageFilter.Result.ACCEPT
        f.accept(0, "abc0", null) == MessageFilter.Result.REJECT
    }

    def "Regex with trailing slash"() {
        f.routingKey = "/[a-z]+/"
        f.init(null)

        expect:
        f.accept(0, "abc", null) == MessageFilter.Result.ACCEPT
        f.accept(0, "abc0", null) == MessageFilter.Result.REJECT
    }

    def "Match exact word"() {
        f.routingKey = "foo"
        f.init(null)

        expect:
        f.accept(0, "foo", null) == MessageFilter.Result.ACCEPT
        f.accept(0, "foo0", null) == MessageFilter.Result.REJECT
    }

    def "Match exact words"() {
        f.routingKey = "foo.bar"
        f.init(null)

        expect:
        f.accept(0, "foo.bar", null) == MessageFilter.Result.ACCEPT
        f.accept(0, "foo0bar", null) == MessageFilter.Result.REJECT
    }

    def "Match any single word"() {
        f.routingKey = "*"
        f.init(null)

        expect:
        f.accept(0, "foo", null) == MessageFilter.Result.ACCEPT
        f.accept(0, "foo.bar", null) == MessageFilter.Result.REJECT
    }

    def "Match any single word at end"() {
        f.routingKey = "foo.*"
        f.init(null)

        expect:
        f.accept(0, "foo.bar", null) == MessageFilter.Result.ACCEPT
        f.accept(0, "foo.", null) == MessageFilter.Result.ACCEPT
        f.accept(0, "foo", null) == MessageFilter.Result.REJECT
        f.accept(0, "foo0.bar", null) == MessageFilter.Result.REJECT
    }

    def "Match any single word in middle"() {
        f.routingKey = "foo.*.baz"
        f.init(null)

        expect:
        f.accept(0, "foo.bar.baz", null) == MessageFilter.Result.ACCEPT
        f.accept(0, "foo..baz", null) == MessageFilter.Result.ACCEPT
        f.accept(0, "foo.bar", null) == MessageFilter.Result.REJECT
        f.accept(0, "foo.bar.baz0", null) == MessageFilter.Result.REJECT
    }

    def "Match any two words"() {
        f.routingKey = "foo.*.*"
        f.init(null)

        expect:
        f.accept(0, "foo.bar.baz", null) == MessageFilter.Result.ACCEPT
        f.accept(0, "foo..baz", null) == MessageFilter.Result.ACCEPT
        f.accept(0, "foo.bar", null) == MessageFilter.Result.REJECT
        f.accept(0, "foo.bar.baz.", null) == MessageFilter.Result.REJECT
    }

    def "Match one or more words"() {
        f.routingKey = "#"
        f.init(null)

        expect:
        f.accept(0, "foo", null) == MessageFilter.Result.ACCEPT
        f.accept(0, "foo.bar", null) == MessageFilter.Result.ACCEPT
    }

    def "Match one or more words at end"() {
        f.routingKey = "foo.#"
        f.init(null)

        expect:
        f.accept(0, "foo.bar", null) == MessageFilter.Result.ACCEPT
        f.accept(0, "foo.bar.baz", null) == MessageFilter.Result.ACCEPT
        f.accept(0, "foo", null) == MessageFilter.Result.REJECT
        f.accept(0, "foo0.bar", null) == MessageFilter.Result.REJECT
    }

    def "Match one or more words in middle"() {
        f.routingKey = "foo.#.bar"
        f.init(null)

        expect:
        f.accept(0, "foo.a.bar", null) == MessageFilter.Result.ACCEPT
        f.accept(0, "foo.a.b.bar", null) == MessageFilter.Result.ACCEPT
        f.accept(0, "foo", null) == MessageFilter.Result.REJECT
        f.accept(0, "foo.bar", null) == MessageFilter.Result.REJECT
    }
}
