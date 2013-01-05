package io.qdb.server.repo;

/**
 * Wraps a transaction id.
 */
public class TxId {

    public long id;

    public TxId() { }

    public TxId(long id) {
        this.id = id;
    }
}
