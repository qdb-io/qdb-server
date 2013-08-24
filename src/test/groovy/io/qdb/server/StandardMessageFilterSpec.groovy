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
import io.qdb.server.filter.StandardMessageFilter
import io.qdb.server.model.Queue
import spock.lang.Specification

class StandardMessageFilterSpec extends Specification {

    StandardMessageFilter f = new StandardMessageFilter()
    Queue q = new Queue()

    def "RoutingKey checked first"() {
        f.routingKey = "foo"
        f.grep = "[a-z]+"
        f.init(q)

        expect:
        f.accept(0, 0, "bar", null) == MessageFilter.Result.REJECT
        f.accept(0, 0, "foo", null) == MessageFilter.Result.CHECK_PAYLOAD
        f.accept(0, 0, "foo", "abc".getBytes("UTF8")) == MessageFilter.Result.ACCEPT
        f.accept(0, 0, "foo", "123".getBytes("UTF8")) == MessageFilter.Result.REJECT
    }
}
