package io.qdb.server.repo;

import io.qdb.server.model.ModelObject;

/**
 * A repository update.
 */
class RepoTx {

    enum Operation { CREATE, UPDATE }
    enum Type { USER, DATABASE, QUEUE }

    public Operation op;
    public Type type;
    public ModelObject object;

    public RepoTx() { }

    public RepoTx(Operation op, Type type, ModelObject object) {
        this.op = op;
        this.type = type;
        this.object = object;
    }

    @Override
    public String toString() {
        return op + " " + type + " " + object;
    }
}
