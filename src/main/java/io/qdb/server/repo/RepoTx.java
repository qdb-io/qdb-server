package io.qdb.server.repo;

import io.qdb.server.model.ModelObject;

/**
 * A repository update.
 */
public class RepoTx {

    enum Operation { CREATE, UPDATE, DELETE }

    public Operation op;
    public ModelObject object;

    public RepoTx() { }

    public RepoTx(Operation op, ModelObject object) {
        this.op = op;
        this.object = object;
    }

    @Override
    public String toString() {
        return op + " " + object;
    }
}
