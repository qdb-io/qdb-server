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
            sent << new Delivery(to: to, msg: msg, from: from, dest: deliverTo[to])
        }

        void deliver(String step) {
            println("\nDelivering " + step + ":")
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

    def setup() {
        transport.sent = []
    }

}
