package io.qdb.server.repo.cluster

import spock.lang.Shared

/**
 * Base class for Paxos specs using shared servers i.e. each server's state persists between tests.
 */
class PaxosSharedBase extends PaxosBase {

    @Shared Paxos s1 = new Paxos<Integer>(1, transport, new SeqNoFactory(1), msgFactory, listener1)
    @Shared Paxos s2 = new Paxos<Integer>(2, transport, new SeqNoFactory(2), msgFactory, listener2)
    @Shared Paxos s3 = new Paxos<Integer>(3, transport, new SeqNoFactory(3), msgFactory, listener3)
    @Shared Map<Object, Paxos<Integer>> servers = [1: s1, 2: s2, 3: s3]

    def setupSpec() {
        transport.deliverTo = servers
        s1.nodes = [1, 2, 3] as Object[]
        s2.nodes = [1, 2, 3] as Object[]
        s3.nodes = [1, 2, 3] as Object[]
    }

}
