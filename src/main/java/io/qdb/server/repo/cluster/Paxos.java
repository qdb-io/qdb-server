package io.qdb.server.repo.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Paxos implementation. This is independent of the msg transport and the sequence number implementation.
 */
public class Paxos<V, N extends Comparable<N>> {

    private static final Logger log = LoggerFactory.getLogger(Paxos.class);

    private final Object self;
    private final Transport transport;
    private final SequenceNoFactory<N> sequenceNoFactory;
    private final MsgFactory<V, N> msgFactory;

    private Object[] nodes;
    private Msg<V,N>[] promised;        // highest numbered PROMISE received from each node PREPARE's sent

    private N highestSeqNoSeen;
    private V v;

    public Paxos(Object self, Transport transport, SequenceNoFactory<N> sequenceNoFactory, MsgFactory<V, N> msgFactory) {
        this.self = self;
        this.transport = transport;
        this.sequenceNoFactory = sequenceNoFactory;
        this.msgFactory = msgFactory;
    }

    /**
     * Set the list of nodes we know about. Any election already in progress is cancelled if the list of nodes has
     * actually changed.
     */
    public synchronized void setNodes(Object[] nodes) {
        if (this.nodes == null || !Arrays.equals(this.nodes, nodes)) {
            int i;
            for (i = nodes.length - 1; i >= 0 && !self.equals(nodes[i]); i--);
            if (i < 0) throw new IllegalArgumentException(self + " not in nodes " + Arrays.asList(nodes));
            this.nodes = nodes;
            // todo cancel any election already in progress
            promised = null;
        }
    }

    /**
     * Start the Paxos algorithm. Any election already in progress is restarted.
     */
    @SuppressWarnings({"StatementWithEmptyBody", "unchecked"})
    public synchronized void propose(V proposal) {
        this.promised = new Msg[nodes.length];
        Msg<V, N> prepare = msgFactory.create(Msg.Type.PREPARE, sequenceNoFactory.next(highestSeqNoSeen), proposal, null);
        for (Object node : nodes) transport.send(node, prepare);
    }

    /**
     * A message has been received from another Node.
     */
    @SuppressWarnings("StatementWithEmptyBody")
    public synchronized void onMessageReceived(Object from, Msg<V, N> msg) {
        switch (msg.getType()) {
            case PREPARE:   onPrepareReceived(from, msg);           break;
            case PROMISE:   onPromiseReceived(from, msg);           break;
            case ACCEPT:    onAcceptReceived(from, msg);            break;
            default:
                throw new IllegalArgumentException("Unknown msg type: " + msg);
        }
    }

    private int indexOfNode(Object node) {
        if (nodes != null) {
            for (int i = nodes.length - 1; i >= 0; i--) if (node.equals(nodes[i])) return i;
        }
        return -1;
    }

    private void onPrepareReceived(Object from, Msg<V, N> msg) {
        N n = msg.getN();
        if (highestSeqNoSeen == null) {
            // haven't seen any proposals so accept this one
            highestSeqNoSeen = n;
            v = msg.getV();
            transport.send(from, msgFactory.create(Msg.Type.PROMISE, n, v, highestSeqNoSeen));

        } else if (n.compareTo(highestSeqNoSeen) < 0) {
            // proposal has lower sequence no so NACK it and include our highest seq no
            transport.send(from, msgFactory.create(Msg.Type.NACK, highestSeqNoSeen, null, null));

        } else {
            // proposal has higher sequence so send back previous highest sequence and it's proposal
            Msg<V, N> ack = msgFactory.create(Msg.Type.PROMISE, n, v, highestSeqNoSeen);
            highestSeqNoSeen = n;
            transport.send(from, ack);
        }
    }

    private void onPromiseReceived(Object from, Msg<V, N> msg) {
        int i = indexOfNode(from);
        if (i < 0) {
            log.warn("PROMISE received from node " + from + " not known to us, ignoring: " + msg);
            return;
        }
        if (promised == null) return;  // ACCEPT already sent

        Msg<V, N> prev = promised[i];
        if (prev == null || prev.getN().compareTo(msg.getN()) < 0) {
            promised[i] = msg;
            // see if we have a majority of PROMISEs + find the most recent (in proposal number ordering) value
            N highest = null;
            V value = null;
            int count = 0;
            for (Msg<V, N> m : promised) {
                if (m == null || m.getV() == null) continue;
                ++count;
                if (highest == null || highest.compareTo(m.getNv()) < 0) {
                    highest = m.getNv();
                    value = m.getV();
                }
            }
            if (count > nodes.length / 2) {
                promised = null;
                Msg<V, N> accept = msgFactory.create(Msg.Type.ACCEPT, highest, value, null);
                for (Object node : nodes) transport.send(node, accept);
            }
        }
    }

    private void onAcceptReceived(Object from, Msg<V, N> msg) {
        // ignore if we have already PROMISEd for a higher sequence no
        if (highestSeqNoSeen != null && highestSeqNoSeen.compareTo(msg.getN()) > 0) return;

        highestSeqNoSeen = msg.getN();
        v = msg.getV();

        Msg<V, N> accepted = msgFactory.create(Msg.Type.ACCEPTED, highestSeqNoSeen, v, null);
        for (Object node : nodes) transport.send(node, accepted);
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
        enum Type { PREPARE, PROMISE, NACK, ACCEPT, ACCEPTED }
        public Type getType();
        public N getN();
        public V getV();
        public N getNv();
    }
}
