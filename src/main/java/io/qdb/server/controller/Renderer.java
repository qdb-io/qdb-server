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

    public void json(Response resp, Object o) throws IOException {
        byte[] bytes = jsonService.toJson(o);
        resp.set("Content-Type", "application/json;charset=utf-8");
        resp.setContentLength(bytes.length);
        resp.getOutputStream().write(bytes);
    }

    public void setCode(Response resp, int code, String message) throws IOException {
        resp.setCode(code);
        json(resp, new StatusMsg(code, message == null ? toMessage(code) : message));
    }

    public void setCode(Response resp, int code, Object data) throws IOException {
        resp.setCode(code);
        json(resp, data);
    }

    private String toMessage(int code) {
        switch (code) {
            case 200:   return "OK";
            case 400:   return "Bad request";
            case 403:   return "Forbidden";
            case 404:   return "Not found";
            case 409:   return "Version mismatch";
        }
        return null;
    }

    private static class StatusMsg {

        public int responseCode;
        public String message;

        private StatusMsg(int responseCode, String message) {
            this.responseCode = responseCode;
            this.message = message;
        }
    }
}
