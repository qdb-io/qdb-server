package io.qdb.server.controller.cluster;

import io.qdb.server.controller.*;
import io.qdb.server.security.AuthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.xml.bind.DatatypeConverter;
import java.io.IOException;

/**
 * Adds clustering related endpoints (status and kvstore).
 */
@Singleton
public class ClusterRouter extends Router {

    private static final Logger log = LoggerFactory.getLogger(ClusterRouter.class);

    private final KvStoreController kvStoreController;
    private final String clusterName;
    private final String clusterPassword;

    @Inject
    public ClusterRouter(AuthService authService, Renderer renderer, ServerStatusController serverStatusController,
                DatabaseController databaseController, UserController userController,
                KvStoreController kvStoreController,
                @Named("clusterName") String clusterName,
                @Named("clusterPassword") String clusterPassword) {
        super(authService, renderer, serverStatusController, databaseController, userController);
        this.kvStoreController = kvStoreController;
        this.clusterName = clusterName;
        this.clusterPassword = clusterPassword;
    }

    @Override
    protected void handleCluster(Call call) throws IOException {
        if (isAuthenticated(call)) {
            String seg = call.nextSegment();
            if ("kvstore".equals(seg)) {
                kvStoreController.handle(call);
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
