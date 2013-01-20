package io.qdb.server.repo.cluster

import spock.lang.Stepwise

@Stepwise
class PaxosPhase1Spec extends PaxosSharedBase {

    def "PREPARE sent to all nodes on start"() {
        s1.propose("p1")

        expect:
        transport.sent() == "PREPARE n=11 v=p1 to 1, PREPARE n=11 v=p1 to 2, PREPARE n=11 v=p1 to 3"
    }

    def "PROMISE sent when no previous proposal seen"() {
        s2.onMessageReceived(1, new Msg(Paxos.Msg.Type.PREPARE, 11, "p1"))

        expect:
        transport.sent() == "PROMISE n=11 v=p1 nv=11 to 1"
    }

    def "NACK sent when higher numbered proposal already seen"() {
        s2.onMessageReceived(1, new Msg(Paxos.Msg.Type.PREPARE, 1, "p1"))

        expect:
        transport.sent() == "NACK n=11 to 1"
    }

    def "PROMISE sent when new proposal has higher number"() {
        s2.onMessageReceived(1, new Msg(Paxos.Msg.Type.PREPARE, 21, "p2"))

        expect:
        transport.sent() == "PROMISE n=21 v=p1 nv=11 to 1"
    }

}