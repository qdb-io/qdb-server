package io.qdb.server.repo.paxos;

/**
 * A unique sequence number. The most significant portion is the transaction id of the server generating the number
 * to ensure that only servers with the highest txId can win the master election.
 */
public class SequenceNo implements Comparable<SequenceNo> {

    public long txId;
    public int seq;
    public String serverId;

    public SequenceNo() { }

    public SequenceNo(long txId, int seq, String serverId) {
        this.txId = txId;
        this.seq = seq;
        this.serverId = serverId;
    }

    @Override
    public int compareTo(SequenceNo o) {
        if (txId != o.txId) return txId < o.txId ? -1 : +1;
        if (seq != o.seq) return seq < o.seq ? -1 : +1;
        return serverId.compareTo(o.serverId);
    }
}
