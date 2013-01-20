package io.qdb.server.repo.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Paxos implementation. This is independent of the msg transport and the sequence number implementation.
 */
public class Paxos<N extends Comparable<N>> {

    private static final Logger log = LoggerFactory.getLogger(Paxos.class);

    private final Object self;
    private final Transport transport;
    private final SequenceNoFactory<N> sequenceNoFactory;
    private final MsgFactory<N> msgFactory;
    private final Listener listener;

    private Object[] nodes;

    private Msg<N>[] promised;        // highest numbered PROMISE received from each node
    private N highestSeqNoSeen;
    private Object v;

    public Paxos(Object self, Transport transport, SequenceNoFactory<N> sequenceNoFactory, MsgFactory<N> msgFactory,
                Listener listener) {
        this.self = self;
        this.transport = transport;
        this.sequenceNoFactory = sequenceNoFactory;
        this.msgFactory = msgFactory;
        this.listener = listener;
    }

    /**
     * Set the list of nodes we know about. Any election already in progress is cancelled if the list of nodes has
     * actually changed.
     */
    @SuppressWarnings("StatementWithEmptyBody")
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
    @SuppressWarnings("unchecked")
    public synchronized void propose(Object proposal) {
        this.promised = new Msg[nodes.length];
        Msg<N> prepare = msgFactory.create(Msg.Type.PREPARE, sequenceNoFactory.next(highestSeqNoSeen), proposal, null);
        for (Object node : nodes) send(prepare, node);
    }

    private void send(Msg<N> msg, Object node) {
        if (log.isDebugEnabled()) log.debug("Sending " + msg + " to " + node);
        transport.send(node, msg, self);
    }

    /**
     * A message has been received from another Node.
     */
    @SuppressWarnings("StatementWithEmptyBody")
    public synchronized void onMessageReceived(Object from, Msg<N> msg) {
        if (log.isDebugEnabled()) log.debug("Received " + msg + "  from " + from);
        switch (msg.getType()) {
            case PREPARE:   onPrepareReceived(from, msg);     break;
            case PROMISE:   onPromiseReceived(from, msg);     break;
            case NACK:      onNackReceived();                 break;
            case ACCEPT:    onAcceptReceived(msg);            break;
            case ACCEPTED:  onAcceptedReceived(msg);          break;
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

    private void onPrepareReceived(Object from, Msg<N> msg) {
        N n = msg.getN();
        if (highestSeqNoSeen == null) {
            // haven't seen any proposals so accept this one
            highestSeqNoSeen = n;
            v = msg.getV();
            send(msgFactory.create(Msg.Type.PROMISE, n, v, highestSeqNoSeen), from);

        } else if (n.compareTo(highestSeqNoSeen) < 0) {
            // proposal has lower sequence no so NACK it and include our highest seq no
            send(msgFactory.create(Msg.Type.NACK, highestSeqNoSeen, null, null), from);

        } else {
            // proposal has higher sequence so send back previous highest sequence and it's proposal
            Msg<N> ack = msgFactory.create(Msg.Type.PROMISE, n, v, highestSeqNoSeen);
            highestSeqNoSeen = n;
            send(ack, from);
        }
    }

    private void onPromiseReceived(Object from, Msg<N> msg) {
        int i = indexOfNode(from);
        if (i < 0) {
            log.warn("PROMISE received from node " + from + " not known to us, ignoring: " + msg);
            return;
        }
        if (promised == null) return;  // ACCEPT already sent or proposal abandoned

        Msg<N> prev = promised[i];
        if (prev == null || prev.getN().compareTo(msg.getN()) < 0) {
            promised[i] = msg;
            // see if we have a majority of PROMISEs + find the most recent (in proposal number ordering) value
            N highest = null;
            Object value = null;
            int count = 0;
            for (Msg<N> m : promised) {
                if (m == null || m.getV() == null) continue;
                ++count;
                if (highest == null || highest.compareTo(m.getNv()) < 0) {
                    highest = m.getNv();
                    value = m.getV();
                }
            }
            if (count > nodes.length / 2) {
                promised = null;
                Msg<N> accept = msgFactory.create(Msg.Type.ACCEPT, highest, value, null);
                for (Object node : nodes) send(accept, node);
            }
        }
    }

    private void onNackReceived() {
        // abandon our proposal
        promised = null;
    }

    private void onAcceptReceived(Msg<N> msg) {
        // ignore if we have already PROMISEd for a higher sequence no
        if (highestSeqNoSeen != null && highestSeqNoSeen.compareTo(msg.getN()) > 0) return;

        highestSeqNoSeen = msg.getN();
        v = msg.getV();

        Msg<N> accepted = msgFactory.create(Msg.Type.ACCEPTED, highestSeqNoSeen, v, null);
        for (Object node : nodes) send(accepted, node);
    }

    private void onAcceptedReceived(Msg<N> msg) {
        highestSeqNoSeen = msg.getN();
        v = msg.getV();
        listener.accepted(v);
    }

    /** Sends messages to nodes asynchronously. */
    public interface Transport {
        void send(Object to, Msg msg, Object from);
    }

    public interface SequenceNoFactory<N extends Comparable<N>> {
        /** Generate a unique sequence number higher than n (which may be null). */
        public N next(N n);
    }

    /** Notified of the progress of the algorithm and the final result (if any). */
    public interface Listener {
        void accepted(Object v);
    }

    /** Creates messages. */
    public interface MsgFactory<N extends Comparable<N>> {
        Msg<N> create(Msg.Type type, N n, Object v, N nv);
    }

    public interface Msg<N extends Comparable> {
        enum Type { PREPARE, PROMISE, NACK, ACCEPT, ACCEPTED }
        public Type getType();
        public N getN();
        public Object getV();
        public N getNv();
    }
}
