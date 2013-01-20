package io.qdb.server.repo.cluster

import spock.lang.Stepwise

@Stepwise
class PaxosCompleteSpec extends PaxosBase {

    def setupSpec() {
        transport.deliverTo = servers
    }

    def "Single proposal accepted"() {
        s1.propose("p1")

        expect:
        listener1.accepted == "p1"
        listener2.accepted == "p1"
        listener3.accepted == "p1"
    }

}