package io.qdb.server.repo.cluster

import spock.lang.Stepwise

@Stepwise
class PaxosCompleteSpec extends PaxosBase {

    def "Single proposal accepted"() {
        s1.propose("p1")
        for (int i = 0; i < 10 && transport.sent; i++) {
            println("Step " + i)
            transport.deliver()
        }

        expect:
        listener1.accepted == "p1"
        listener2.accepted == "p1"
        listener3.accepted == "p1"
    }

}