package io.qdb.server.controller;

import java.io.IOException;

/**
 * Responds to requests with a status code.
 */
public class StatusCodeController implements Controller {

    public static final StatusCodeController SC_404 = new StatusCodeController(404);
    public static final StatusCodeController SC_403 = new StatusCodeController(403);

    private final int code;

    private StatusCodeController(int code) {
        this.code = code;
    }

    @Override
    public void handle(Call call) throws IOException {
        call.setCode(code);
    }
}
