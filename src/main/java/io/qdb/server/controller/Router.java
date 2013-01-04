package io.qdb.server.controller;

import com.google.inject.Inject;
import io.qdb.server.controller.cluster.ClusterController;
import io.qdb.server.model.Repository;
import io.qdb.server.security.Auth;
import io.qdb.server.security.AuthService;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.IOException;

/**
 * Routes requests to controllers for processing.
 */
@Singleton
public class Router implements Container {

    private static final Logger log = LoggerFactory.getLogger(Router.class);

    private final AuthService authService;
    private final Renderer renderer;
    private final ServerStatusController serverStatusController;
    private final DatabaseController databaseController;
    private final UserController userController;

    @Inject(optional = true)
    private ClusterController clusterController;

    @Inject
    public Router(AuthService authService, Renderer renderer, ServerStatusController serverStatusController,
                  DatabaseController databaseController, UserController userController,
                  ClusterController clusterController) {
        this.authService = authService;
        this.renderer = renderer;
        this.serverStatusController = serverStatusController;
        this.databaseController = databaseController;
        this.userController = userController;
        this.clusterController = clusterController;
    }

    @Override
    public void handle(Request req, Response resp) {
        try {
            Call call = new Call(req, resp, renderer);
            String seg = call.nextSegment();
            if ("cluster".equals(seg)) {
                if (clusterController == null) {
                    call.setCode(404, "Clustering is disabled");
                } else {
                    clusterController.handle(call);
                }
            } else {
                Auth auth = authService.authenticate(req, resp);
                if (auth == null) {
                    authService.sendChallenge(resp);
                } else {
                    call.setAuth(auth);
                    if (seg == null) {
                        serverStatusController.handle(call);
                    } else if (call.getAuth().isAnonymous()) {
                        authService.sendChallenge(resp);
                    } else if ("databases".equals(seg)) {
                        databaseController.handle(call);
                    } else if ("users".equals(seg)) {
                        userController.handle(call);
                    } else {
                        if (call.isGet()) call.setCode(404);
                        else call.setCode(400);
                    }
                }
            }
        } catch (Repository.UnavailableException e) {
            if (log.isDebugEnabled()) log.debug("503: " + req.getPath() + " " + e.getMessage());
            quietRenderCode(req, resp, 503, e.getMessage());
        } catch (Exception e) {
            log.error("500: " + req.getPath() + " " + e, e);
            quietRenderCode(req, resp, 500, null);
        }
        try {
            resp.close();
        } catch (IOException x) {
            if (log.isDebugEnabled()) log.debug("Error closing response: " + x, x);
        }
    }

    private void quietRenderCode(Request req, Response resp, int code, String msg) {
        try {
            renderer.setCode(resp, code, msg);
        } catch (IOException x) {
            if (log.isDebugEnabled()) log.debug(req.getPath() + " " + x.getMessage(), x);
        }
    }
}
