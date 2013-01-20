package io.qdb.server.repo.cluster

/**
 * Base class for Paxos specs using non-shared servers i.e. a new set of servers is created before each test.
 */
class PaxosNonSharedBase extends PaxosBase {

    Paxos s1 = new Paxos<Integer>(1, transport, new SeqNoFactory(1), msgFactory, listener1)
    Paxos s2 = new Paxos<Integer>(2, transport, new SeqNoFactory(2), msgFactory, listener2)
    Paxos s3 = new Paxos<Integer>(3, transport, new SeqNoFactory(3), msgFactory, listener3)
    Map<Object, Paxos<Integer>> servers = [1: s1, 2: s2, 3: s3]

    def setup() {
        transport.deliverTo = servers
        s1.nodes = [1, 2, 3] as Object[]
        s2.nodes = [1, 2, 3] as Object[]
        s3.nodes = [1, 2, 3] as Object[]
    }

}
