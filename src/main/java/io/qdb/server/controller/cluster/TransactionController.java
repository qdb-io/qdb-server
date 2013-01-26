package io.qdb.server.controller.cluster;

import io.qdb.buffer.MessageCursor;
import io.qdb.server.OurServer;
import io.qdb.server.controller.Call;
import io.qdb.server.controller.CrudController;
import io.qdb.server.controller.JsonService;
import io.qdb.server.controller.MessageController;
import io.qdb.server.repo.*;
import io.qdb.server.repo.cluster.ClusterException;
import io.qdb.server.repo.cluster.ClusteredRepository;
import io.qdb.server.repo.cluster.SlaveRegistry;
import org.simpleframework.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Slaves POST new meta-data update transactions to the master and stream new transactions from the masters
 * transaction log queue.
 */
@Singleton
public class TransactionController extends CrudController {

    private static final Logger log = LoggerFactory.getLogger(TransactionController.class);

    private final ClusteredRepository repo;
    private final OurServer ourServer;
    private final SlaveRegistry slaveRegistry;
    private final int clusterTimeoutMs;

    @Inject
    public TransactionController(JsonService jsonService, ClusteredRepository clusteredRepository, OurServer ourServer,
                SlaveRegistry slaveRegistry,
                @Named("clusterTimeoutMs") int clusterTimeoutMs) {
        super(jsonService);
        this.repo = clusteredRepository;
        this.ourServer = ourServer;
        this.slaveRegistry = slaveRegistry;
        this.clusterTimeoutMs = clusterTimeoutMs;
    }

/*
    */
/**
     * POST a new transaction. Returns 201 and a TxId JSON object on success. Returns 410 (not longer the master)
     * or 409 (ModelException) with a text response otherwise.
     *//*

    @Override
    protected void create(Call call) throws IOException {
        RepoTx tx = getBodyObject(call, RepoTx.class);
        try {
            call.setCode(201, new TxId(repo.appendTxFromSlave(tx)));
        } catch (ClusterException.NotMaster e) {
            if (log.isDebugEnabled()) log.debug(e.toString());
            call.setText(410, "Not the master " + ourServer);
        } catch (ModelException e) {
            if (log.isDebugEnabled()) log.debug(e.toString(), e);
            call.setText(409, e.getMessage());
        }
    }

    */
/**
     * Stream transactions from id onwards.
     *//*

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        long txId = call.getLong("txId", -1L);
        if (txId < 0) {
            call.setText(400, "Missing txId");
            return;
        }

        String repositoryId = call.getString("repositoryId", null);
        if (repositoryId == null) {
            call.setText(400, "Missing repositoryId");
            return;
        }
        if (!repositoryId.equals(repo.getRepositoryId())) {
            call.setText(409, "Invalid repositoryId");
            return;
        }

        int keepAliveMs = call.getInt("keepAlive", clusterTimeoutMs / 2);
        if (keepAliveMs <= 100) {
            call.setText(400, "Invalid keepAliveMs: " + keepAliveMs);
            return;
        }

        String slaveId = call.getRequest().getValue("Referer");
        if (slaveId == null) {
            call.setText(400, "Missing 'Referer' HTTP Header");
            return;
        }

        MessageCursor c;
        try {
            c = repo.openTxCursor(txId);
        } catch (ClusterException.NotMaster e) {
            if (log.isDebugEnabled()) log.debug(e.toString());
            call.setText(410, "Not the master " + ourServer);
            return;
        }

        SlaveRegistry.Slave slave = slaveRegistry.slaveConnected(slaveId, txId);
        try {
            Response response = call.getResponse();
            response.set("Content-Type", "text/plain");
            OutputStream out = response.getOutputStream();
            outer:
            while (true) {
                try {
                    for (;;) {
                        try {
                            if (c.next(keepAliveMs)) break;
                        } catch (IOException e) {
                            log.error("Error getting transaction for " + slave + ": " + e, e);
                            break outer;
                        }
                        out.write(10);
                        out.flush();
                        slave.active(txId);
                    }
                } catch (InterruptedException e) {
                    break;
                }

                MessageController.MessageHeader h = new MessageController.MessageHeader(c);
                txId = h.id;
                if (log.isDebugEnabled()) log.debug("Sending txId " + txId + " to slave " + slave);
                out.write(jsonService.toJsonNoIndenting(h));
                out.write(10);
                out.write(c.getPayload());
                out.write(10);
                out.flush();
                slave.active(txId);
            }
        } catch (IOException e) {
            Throwable t , n;
            for (t = e; (n = t.getCause()) != null; t = n);
            slave.setErrorMessage(t.toString());
        } finally {
            try {
                c.close();
            } catch (IOException ignore) {
            }
            slave.disconnected();
        }
    }
*/
}
