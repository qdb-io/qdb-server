/*
 * Copyright 2013 David Tinker
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
            Call call = new Call(req, resp, renderer);
            Auth auth = authService.authenticate(req, resp);
            if (auth == null) {
                authService.sendChallenge(resp);
            } else {
                call.setAuth(auth);
                String seg = call.nextSegment();
                if (seg == null) {
                    serverStatusController.handle(call);
                } else if (call.getAuth().isAnonymous()) {
                    authService.sendChallenge(resp);
                } else if ("db".equals(seg)) {
                    databaseController.handle(call);
                } else if ("users".equals(seg)) {
                    userController.handle(call);
                } else {
                    if (call.isGet()) call.setCode(404);
                    else call.setCode(400);
                }
            }
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
