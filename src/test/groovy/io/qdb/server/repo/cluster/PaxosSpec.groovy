package io.qdb.server.repo.cluster

import spock.lang.Stepwise

@Stepwise
class PaxosSpec extends PaxosBase {

    def "Initial proposal sent"() {
        s1.start([1, 2, 3] as Object[], "p1")

        expect:
        transport.sent.toString() == "[PREPARE n=11 v=p1 to 2, PREPARE n=11 v=p1 to 3]"
    }

    def "ACK sent as no previous proposal seen"() {
        def prepare = transport.sent[0].msg
        transport.sent = []
        s2.onMessageReceived(1, prepare)

        expect:
        transport.sent.toString() == "[ACK n=11 to 1]"
    }

    def "NACK sent as higher numbered proposal already seen"() {
        transport.sent = []
        s2.onMessageReceived(1, new Msg(Paxos.Msg.Type.PREPARE, 1, "p1", null))

        expect:
        transport.sent.toString() == "[NACK n=11 to 1]"
    }

}