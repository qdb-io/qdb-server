package io.qdb.server.controller.cluster;

import io.qdb.server.ServerId;
import io.qdb.server.controller.Call;
import io.qdb.server.controller.CrudController;
import io.qdb.server.controller.JsonService;
import io.qdb.server.model.ModelException;
import io.qdb.server.repo.ClusterException;
import io.qdb.server.repo.ClusteredRepository;
import io.qdb.server.repo.RepoTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
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
    private final String host;
    private final String serverId;

    @Inject
    public TransactionController(JsonService jsonService, ClusteredRepository clusteredRepository, ServerId serverId,
            @Named("host") String host) {
        super(jsonService);
        this.clusteredRepository = clusteredRepository;
        this.host = host;
        this.serverId = serverId.get();
    }

    @Override
    protected void create(Call call) throws IOException {
        RepoTx tx = getBodyObject(call, RepoTx.class);
        try {
            clusteredRepository.appendTxFromSlave(tx);
            call.setCode(201);
        } catch (ClusterException.NotMaster e) {
            if (log.isDebugEnabled()) log.debug(e.toString());
            call.setCode(410, "Not the master (" + host + " " + serverId + ")");
        } catch (ModelException e) {
            if (log.isDebugEnabled()) log.debug(e.toString(), e);
            call.setCode(409, e.getMessage());
        }
    }
}
