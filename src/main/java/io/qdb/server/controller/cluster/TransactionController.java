package io.qdb.server.controller.cluster;

import io.qdb.server.OurServer;
import io.qdb.server.controller.Call;
import io.qdb.server.controller.CrudController;
import io.qdb.server.controller.JsonService;
import io.qdb.server.model.ModelException;
import io.qdb.server.repo.ClusterException;
import io.qdb.server.repo.ClusteredRepository;
import io.qdb.server.repo.RepoTx;
import io.qdb.server.repo.TxId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

/**
 * Slaves POST new meta-data update transactions to the master and read new transactions from the masters
 * transaction log queue.
 */
@Singleton
public class TransactionController extends CrudController {

    private static final Logger log = LoggerFactory.getLogger(TransactionController.class);

    private final ClusteredRepository clusteredRepository;
    private final OurServer ourServer;

    @Inject
    public TransactionController(JsonService jsonService, ClusteredRepository clusteredRepository, OurServer ourServer) {
        super(jsonService);
        this.clusteredRepository = clusteredRepository;
        this.ourServer = ourServer;
    }

    /**
     * POST a new transaction. Returns 201 and a TxId JSON object on success. Returns 410 (not longer the master)
     * or 409 (ModelException) with a text response otherwise.
     */
    @Override
    protected void create(Call call) throws IOException {
        RepoTx tx = getBodyObject(call, RepoTx.class);
        try {
            call.setCode(201, new TxId(clusteredRepository.appendTxFromSlave(tx)));
        } catch (ClusterException.NotMaster e) {
            if (log.isDebugEnabled()) log.debug(e.toString());
            call.setText(410, "Not the master " + ourServer);
        } catch (ModelException e) {
            if (log.isDebugEnabled()) log.debug(e.toString(), e);
            call.setText(409, e.getMessage());
        }
    }
}
