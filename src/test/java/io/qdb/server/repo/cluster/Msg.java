package io.qdb.server.repo.cluster;

/**
 * For testing Paxos. I had some weird hassles coding this in Groovy which is why it is in Java.
 */
public class Msg implements Paxos.Msg<String, Integer> {

    private final Type type;
    private final Integer n;
    private final String v;
    private final Integer nv;

    public static class Factory implements Paxos.MsgFactory<String, Integer> {
        @Override
        public Paxos.Msg<String, Integer> create(Type type, Integer n, String v, Integer nv) {
            return new Msg(type, n, v, nv);
        }
    }

    public Msg(Type type, Integer n) {
        this(type, n, null, null);
    }

    public Msg(Type type, Integer n, String v) {
        this(type, n, v, null);
    }

    public Msg(Type type, Integer n, String v, Integer nv) {
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

    public String getV() {
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
