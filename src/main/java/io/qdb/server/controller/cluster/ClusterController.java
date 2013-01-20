package io.qdb.server.controller.cluster;

import io.qdb.server.controller.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.xml.bind.DatatypeConverter;
import java.io.IOException;

/**
 * These operations are used by the servers in a QDB cluster to exchange meta data, elect leaders etc..
 */
@Singleton
public class ClusterController implements Controller {

    private static final Logger log = LoggerFactory.getLogger(ClusterController.class);

    private final TransactionController transactionController;
    private final SnapshotController snapshotController;
    private final ServerStatusController serverStatusController;
    private final PaxosController paxosController;
    private final String clusterName;
    private final String clusterPassword;

    @Inject
    public ClusterController(TransactionController transactionController, SnapshotController snapshotController,
                ServerStatusController serverStatusController, PaxosController paxosController,
                @Named("clusterName") String clusterName,
                @Named("clusterPassword") String clusterPassword) {
        this.transactionController = transactionController;
        this.snapshotController = snapshotController;
        this.serverStatusController = serverStatusController;
        this.paxosController = paxosController;
        this.clusterName = clusterName;
        this.clusterPassword = clusterPassword;
    }

    @Override
    public void handle(Call call) throws IOException {
        if (isAuthenticated(call)) {
            String seg = call.nextSegment();
            if ("transactions".equals(seg)) {
                transactionController.handle(call);
            } else if ("status".equals(seg)) {
                serverStatusController.handle(call);
            } else if ("snapshots".equals(seg)) {
                snapshotController.handle(call);
            } else if ("paxos".equals(seg)) {
                paxosController.handle(call);
            } else {
                call.setCode(404);
            }
        } else {
            call.setCode(401);
        }
    }

    private boolean isAuthenticated(Call call) throws IOException {
        String s = call.getRequest().getValue("Authorization");
        if (s == null) return false;

        if (s.startsWith("Basic ")) {
            try {
                s = new String(DatatypeConverter.parseBase64Binary(s.substring(6)), "UTF8");
                int i = s.indexOf(':');
                if (i > 0) {
                    String username = s.substring(0, i);
                    if (!username.equals(clusterName)) {
                        log.warn("Incorrect cluster name [" + clusterName + "] instead of [" + clusterName +
                                "] received from " + call.getRequest().getClientAddress());
                        return false;
                    }
                    String password = s.substring(i + 1);
                    if (!password.equals(clusterPassword)) {
                        log.warn("Incorrect cluster password received from " + call.getRequest().getClientAddress());
                        return false;
                    }
                    return true;

                }
            } catch (IllegalArgumentException ignore) {
            }
        }
        return false;
    }
}
