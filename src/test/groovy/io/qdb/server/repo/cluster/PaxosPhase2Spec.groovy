package io.qdb.server.repo.cluster

import spock.lang.Stepwise

@Stepwise
class PaxosPhase2Spec extends PaxosSharedBase {

    def setupSpec() {
        s1.propose("p1")
    }

    def "No ACCEPT sent before majority of PROMISE's received"() {
        s1.onMessageReceived(2, new Msg(Paxos.Msg.Type.PROMISE, 11, "p1", 11))

        expect:
        transport.sent() == ""
    }

    def "PROMISE from unknown node ignored"() {
        s1.onMessageReceived(4, new Msg(Paxos.Msg.Type.PROMISE, 11, "p1", 11))

        expect:
        transport.sent() == ""
    }

    def "ACCEPT sent with highest proposal when a majority of PROMISE's have been received"() {
        s1.onMessageReceived(3, new Msg(Paxos.Msg.Type.PROMISE, 21, "p2", 21))

        expect:
        transport.sent() == "ACCEPT n=21 v=p2 to 1, ACCEPT n=21 v=p2 to 2, ACCEPT n=21 v=p2 to 3"
    }

    def "PROMISE received after ACCEPT sent ignored"() {
        s1.onMessageReceived(3, new Msg(Paxos.Msg.Type.PROMISE, 21, "p2", 21))

        expect:
        transport.sent() == ""
    }

    def "ACCEPTED sent to all nodes when node receives ACCEPT"() {
        s2.onMessageReceived(1, new Msg(Paxos.Msg.Type.ACCEPT, 21, "p2"))

        expect:
        transport.sent() == "ACCEPTED n=21 v=p2 to 1, ACCEPTED n=21 v=p2 to 2, ACCEPTED n=21 v=p2 to 3"
    }

    def "ACCEPTED not sent when node receives ACCEPT and has made a higher PROMISE"() {
        s3.onMessageReceived(1, new Msg(Paxos.Msg.Type.PREPARE, 21, "p2"));
        transport.sent = []
        s3.onMessageReceived(1, new Msg(Paxos.Msg.Type.ACCEPT, 11, "p1"))

        expect:
        transport.sent() == ""
    }

    def "Listener notified when node receives ACCEPTED"() {
        s2.onMessageReceived(1, new Msg(Paxos.Msg.Type.ACCEPTED, 21, "p2"))

        expect:
        listener2.accepted == "p2"
    }
}