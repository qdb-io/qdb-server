package io.qdb.server.controller;

import io.qdb.server.security.Auth;
import io.qdb.server.security.AuthService;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

/**
 * Routes requests to controllers for processing.
 *
 * / - GET server status
 * /dbs
 * /dbs/$db/queues
 * /dbs/$db/queues/queue_path
 */
@Singleton
public class Router implements Container {

    private static final Logger log = LoggerFactory.getLogger(Router.class);

    private final AuthService authService;
    private final Renderer renderer;
    private final ServerStatusController serverStatusController;
    private final DatabaseController databaseController;
    private final UserController userController;

    @Inject
    public Router(AuthService authService, Renderer renderer, ServerStatusController serverStatusController,
                  DatabaseController databaseController, UserController userController) {
        this.authService = authService;
        this.renderer = renderer;
        this.serverStatusController = serverStatusController;
        this.databaseController = databaseController;
        this.userController = userController;
    }

    @Override
    public void handle(Request req, Response resp) {
        try {
            Auth auth = authService.authenticate(req, resp);
            if (auth == null) {
                authService.sendChallenge(resp);
            } else {
                Call call = new Call(req, resp, auth, renderer);
                String seg = call.nextSegment();
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
        } catch (Exception e) {
            log.error(req.getPath() + " " + e.getMessage(), e);
            try {
                renderer.setCode(resp, 500, null);
            } catch (IOException x) {
                if (log.isDebugEnabled()) log.debug(req.getPath() + " " + e.getMessage(), e);
            }
        }
        try {
            resp.close();
        } catch (IOException x) {
            if (log.isDebugEnabled()) log.debug("Error closing response: " + x, x);
        }
    }
}
