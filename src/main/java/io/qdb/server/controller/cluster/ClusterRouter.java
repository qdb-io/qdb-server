package io.qdb.server.controller.cluster;

import io.qdb.server.controller.*;
import io.qdb.server.security.AuthService;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

/**
 * Adds clustering related endpoints.
 */
@Singleton
public class ClusterRouter extends Router {

    private final ClusterController clusterController;

    @Inject
    public ClusterRouter(AuthService authService, Renderer renderer, ServerStatusController serverStatusController,
                DatabaseController databaseController, UserController userController,
                ClusterController clusterController) {
        super(authService, renderer, serverStatusController, databaseController, userController);
        this.clusterController = clusterController;
    }

    @Override
    protected void handleCluster(Call call) throws IOException {
        clusterController.handle(call);
    }
}
