package io.qdb.server.controller;

import io.qdb.server.JsonService;
import org.simpleframework.http.Response;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

/**
 * Helps render HTTP responses.
 */
@Singleton
public class Renderer {

    protected final JsonService jsonService;

    @Inject
    public Renderer(JsonService jsonService) {
        this.jsonService = jsonService;
    }

    public void renderJson(Call call, Object o) throws IOException {
        byte[] bytes = jsonService.toJson(o);
        Response resp = call.getResponse();
        resp.set("Content-Type", "application/json;charset=utf-8");
        resp.setContentLength(bytes.length);
        resp.getOutputStream().write(bytes);
    }

}
