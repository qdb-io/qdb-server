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

import org.simpleframework.http.Response;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Date;

/**
 * Placeholder that will serve up the Javascript admin UI at some point.
 */
@Singleton
public class AdminUIController implements Controller {

    @Inject
    public AdminUIController() {
    }

    @Override
    public void handle(Call call) throws IOException {
        if (call.isGet()) {
            if (call.getSegments().length == 0) {
                Response resp = call.getResponse();
                resp.setCode(200);
                resp.set("Content-Type", "text/html;charset=utf-8");
                byte[] bytes = PLACEHOLDER_HTML.getBytes("UTF8");
                resp.setContentLength(bytes.length);
                resp.getOutputStream().write(bytes);
            } else {
                call.setCode(404);
            }
        } else {
            call.setCode(400);
        }
    }

    private static final String PLACEHOLDER_HTML =
            "<head>\n" +
            "<title>QDB</title>\n" +
            "</head>\n" +
            "<body>\n" +
            "<p>Admin UI coming soon .. to check the status of the server GET /status</p>\n" +
            "</body>";
}
