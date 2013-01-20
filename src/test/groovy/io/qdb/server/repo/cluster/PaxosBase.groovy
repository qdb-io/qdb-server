package io.qdb.server.repo.cluster

import spock.lang.Shared
import spock.lang.Specification

/**
 * Base class for Paxos specs. Defines implementations of Paxos interfaces and some shared servers etc.
 */
class PaxosBase extends Specification {

    static class Delivery {
        Object to
        Paxos.Msg msg
        Object from
        Paxos dest
        void deliver() { dest.onMessageReceived(from, msg) }
        String toString() { "" + msg + " to " + to }
    }

    class Transport implements Paxos.Transport {
        List<Delivery> sent = []
        Map deliverTo

        void send(Object to, Paxos.Msg msg, Object from) {
            def delivery = new Delivery(to: to, msg: msg, from: from)
            if (deliverTo) delivery.dest = (Paxos)deliverTo[to]
            sent << delivery
        }

        void deliver() {
            def todo = sent
            sent = []
            todo.each { it.deliver() }
        }

        String sent() { def s = sent.toString(); s.substring(1, s.length() - 1) }
    }

    static class SeqNoFactory implements Paxos.SequenceNoFactory<Integer> {
        final int serverNo
        SeqNoFactory(int serverNo) { this.serverNo = serverNo }
        Integer next(Integer n) { return (n == null ? 10 : n - (n % 10)) + serverNo }
    }

    static class Listener implements Paxos.Listener {
        Object accepted
        void accepted(Object v) { this.accepted = v }
    }

    @Shared Msg.Factory msgFactory = new Msg.Factory()
    @Shared Transport transport = new Transport()
    @Shared Listener listener1 = new Listener()
    @Shared Listener listener2 = new Listener()
    @Shared Listener listener3 = new Listener()

    @Shared Paxos s1 = new Paxos<Integer>(1, transport, new SeqNoFactory(1), msgFactory, listener1)
    @Shared Paxos s2 = new Paxos<Integer>(2, transport, new SeqNoFactory(2), msgFactory, listener2)
    @Shared Paxos s3 = new Paxos<Integer>(3, transport, new SeqNoFactory(3), msgFactory, listener3)
    @Shared Map<Object, Paxos<Integer>> servers = [1: s1, 2: s2, 3: s3]

    def setupSpec() {
        s1.nodes = [1, 2, 3] as Object[]
        s2.nodes = [1, 2, 3] as Object[]
        s3.nodes = [1, 2, 3] as Object[]
    }

    def setup() {
        transport.sent = []
    }

}
