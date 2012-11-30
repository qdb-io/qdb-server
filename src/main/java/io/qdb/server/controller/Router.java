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
 * Routes requests to handlers for processing.
 * / - GET server status
 * /$namespace/queues
 * /$namespace/queues/queue_path
 */
@Singleton
public class Router implements Container {

    private static final Logger log = LoggerFactory.getLogger(Router.class);

    private final AuthService authService;
    private final ServerStatusController serverStatusController;

    @Inject
    public Router(AuthService authService, ServerStatusController serverStatusController) {
        this.authService = authService;
        this.serverStatusController = serverStatusController;
    }

    @Override
    public void handle(Request req, Response resp) {
        try {
            Auth auth = authService.authenticate(req, resp);
            if (auth == null) {
                authService.sendChallenge(resp);
            } else {
                Call call = new Call(req, resp, auth);
                if (log.isDebugEnabled()) log.debug(call.toString());

                String[] segments = req.getPath().getSegments();
                if (segments.length == 0) {
                    serverStatusController.index(call);
                } else if (call.getAuth().isAnonymous()) {
                    authService.sendChallenge(resp);
                } else {
                    resp.setCode(404);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            resp.setCode(500);
        }
        try {
            resp.close();
        } catch (IOException x) {
            log.debug("Error closing response: " + x, x);
        }
    }
}
