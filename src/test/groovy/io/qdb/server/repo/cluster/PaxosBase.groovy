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
        String toString() { "" + msg + " to " + to }
    }

    static class Transport implements Paxos.Transport {
        List<Delivery> sent = []
        void send(Object to, Paxos.Msg msg) { sent << new Delivery(to: to, msg: msg) }
    }

    static class SeqNoFactory implements Paxos.SequenceNoFactory<Integer> {
        final int serverNo
        SeqNoFactory(int serverNo) { this.serverNo = serverNo }
        Integer next(Integer highestSeqNoSeen) {
            return (highestSeqNoSeen == null ? 10 : highestSeqNoSeen - (highestSeqNoSeen % 10)) + serverNo
        }
    }

    @Shared Msg.Factory msgFactory = new Msg.Factory()
    @Shared Transport transport = new Transport()

    @Shared Paxos s1 = new Paxos<String, Integer>(1, transport, new SeqNoFactory(1), msgFactory)
    @Shared Paxos s2 = new Paxos<String, Integer>(2, transport, new SeqNoFactory(2), msgFactory)
    @Shared Paxos s3 = new Paxos<String, Integer>(3, transport, new SeqNoFactory(3), msgFactory)

}
