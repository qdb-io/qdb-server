package io.qdb.server.repo.cluster;

/**
 * Paxos implementation. This is independent of the msg transport and the sequence number implementation.
 */
public class Paxos<V, N extends Comparable<N>> {

    private final Object self;
    private final Transport transport;
    private final SequenceNoFactory<N> sequenceNoFactory;
    private final MsgFactory<V, N> msgFactory;

    private Object[] nodes;
    private V ourProposal;

    private N highestSeqNoSeen;
    private V proposal;

    public Paxos(Object self, Transport transport, SequenceNoFactory<N> sequenceNoFactory, MsgFactory<V, N> msgFactory) {
        this.self = self;
        this.transport = transport;
        this.sequenceNoFactory = sequenceNoFactory;
        this.msgFactory = msgFactory;
    }

    /**
     * Start the Paxos algorithm. Any election already in progress is restarted.
     */
    public synchronized void start(Object[] nodes, V proposal) {
        // todo cancel any election already in progress (somehow)
        this.nodes = nodes;
        this.ourProposal = proposal;

        Msg<V, N> prepare = msgFactory.create(Msg.Type.PREPARE, sequenceNoFactory.next(highestSeqNoSeen), proposal, null);
        for (Object node : nodes) {
            if (!self.equals(node)) transport.send(node, prepare);
        }
    }

    /**
     * A message has been received from another Node.
     */
    public synchronized void onMessageReceived(Object from, Msg<V, N> msg) {
        switch (msg.getType()) {
            case PREPARE:
                onPrepareReceived(from, msg);
                break;
            default:
                throw new IllegalArgumentException("Unknown msg type: " + msg);
        }
    }

    private void onPrepareReceived(Object from, Msg<V, N> msg) {
        N n = msg.getN();
        if (highestSeqNoSeen == null) {
            // haven't seen any proposals so accept this one
            highestSeqNoSeen = n;
            proposal = msg.getV();
            // todo stable storage?
            transport.send(from, msgFactory.create(Msg.Type.ACK, n, null, null));

        } else if (n.compareTo(highestSeqNoSeen) < 0) {
            // proposal has lower sequence no so NACK it and include our highest seq no
            transport.send(from, msgFactory.create(Msg.Type.NACK, highestSeqNoSeen, null, null));

        } else {
            // proposal has higher sequence so send back previous highest sequence and it's proposal
            Msg<V, N> ack = msgFactory.create(Msg.Type.ACK, highestSeqNoSeen, proposal, null);
            highestSeqNoSeen = n;
            transport.send(from, ack);
        }
    }

    /** Sends messages to nodes asynchronously. */
    public interface Transport {
        void send(Object to, Msg msg);
    }

    public interface SequenceNoFactory<N extends Comparable<N>> {
        /** Generate a unique sequence number higher than highestSeqNoSeen (which may be null). */
        public N next(N highestSeqNoSeen);
    }

    /** Notified of the progress of the algorithm and the final result (if any). */
    public interface Listener<V> {
    }

    /** Creates messages. */
    public interface MsgFactory<V, N extends Comparable<N>> {
        Msg<V, N> create(Msg.Type type, N n, V v, N nv);
    }

    public interface Msg<V, N extends Comparable> {
        enum Type { PREPARE, ACK, NACK, ACCEPT, ACCEPTED }
        public Type getType();
        public N getN();
        public V getV();
        public N getNv();
    }
}
