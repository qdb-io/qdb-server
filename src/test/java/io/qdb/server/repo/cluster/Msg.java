package io.qdb.server.repo.cluster;

/**
 * For testing Paxos. I had some weird hassles coding this in Groovy which is why it is in Java.
 */
public class Msg implements Paxos.Msg<Integer> {

    private final Type type;
    private final Integer n;
    private final Object v;
    private final Integer nv;

    public static class Factory implements Paxos.MsgFactory<Integer> {
        @Override
        public Paxos.Msg<Integer> create(Type type, Integer n, Object v, Integer nv) {
            return new Msg(type, n, v, nv);
        }
    }

    public Msg(Type type, Integer n) {
        this(type, n, null, null);
    }

    public Msg(Type type, Integer n, Object v) {
        this(type, n, v, null);
    }

    public Msg(Type type, Integer n, Object v, Integer nv) {
        this.type = type;
        this.n = n;
        this.v = v;
        this.nv = nv;
    }

    public Type getType() {
        return type;
    }

    public Integer getN() {
        return n;
    }

    public Object getV() {
        return v;
    }

    public Integer getNv() {
        return nv;
    }

    @Override
    public String toString() {
        return type + (n != null ? " n=" + n : "" ) + (v != null ? " v=" + v : "") + (nv != null ? " nv=" + nv : "");
    }
}
