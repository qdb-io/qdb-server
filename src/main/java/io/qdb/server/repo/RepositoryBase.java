package io.qdb.server.repo;

import io.qdb.server.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Converts repository interface calls that involve updates into RepoTx instances and executes them.
 */
public abstract class RepositoryBase implements Repository {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected RepositoryBase() {
    }

    /**
     * Execute the transaction. For a local repository it is appended to the tx log and applied to the memory model.
     * For a clustered repository it might be sent to the master. Returns the transaction id.
     */
    protected abstract long exec(RepoTx tx) throws IOException;

    @Override
    public User createUser(User user) throws IOException {
        exec(new RepoTx(RepoTx.Operation.CREATE, user));
        return findUser(user.getId());
    }

    @Override
    public User updateUser(User user) throws IOException {
        exec(new RepoTx(RepoTx.Operation.UPDATE, user));
        return findUser(user.getId());
    }

    @Override
    public Database createDatabase(Database db) throws IOException {
        exec(new RepoTx(RepoTx.Operation.CREATE, db));
        return findDatabase(db.getId());
    }

    @Override
    public Database updateDatabase(Database db) throws IOException {
        exec(new RepoTx(RepoTx.Operation.UPDATE, db));
        return findDatabase(db.getId());
    }

    @Override
    public Queue createQueue(Queue queue) throws IOException {
        exec(new RepoTx(RepoTx.Operation.CREATE, queue));
        return findQueue(queue.getId());
    }

    @Override
    public Queue updateQueue(Queue queue) throws IOException {
        exec(new RepoTx(RepoTx.Operation.UPDATE, queue));
        return findQueue(queue.getId());
    }

}
