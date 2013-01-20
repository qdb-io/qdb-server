package io.qdb.server.repo.cluster

import spock.lang.Stepwise

@Stepwise
class PaxosCompleteSpec extends PaxosNonSharedBase {

    def "Single proposal accepted"() {
        s1.propose("p1")
        transport.deliver("PREPARE")
        transport.deliver("PROMISE")
        transport.deliver("ACCEPT")
        transport.deliver("ACCEPTED")

        expect:
        listener1.accepted == "p1"
        listener2.accepted == "p1"
        listener3.accepted == "p1"
    }

}