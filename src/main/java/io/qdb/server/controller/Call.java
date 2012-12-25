package io.qdb.server.controller;

import io.qdb.server.model.Database;
import io.qdb.server.model.Queue;
import io.qdb.server.model.User;
import io.qdb.server.security.Auth;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;

import java.io.IOException;

/**
 * Encapsulates a call to the server. Includes the request, response and authentication information. Adds
 * convenience methods to render responses.
 */
public class Call {

    private final Request request;
    private final Response response;
    private final Auth auth;
    private final String[] segments;
    private final Renderer renderer;

    private int currentSegment;
    private int code = 200;

    private Database database;
    private Queue queue;

    public Call(Request request, Response response, Auth auth, Renderer renderer) {
        this.request = request;
        this.response = response;
        this.auth = auth;
        this.renderer = renderer;
        this.segments = request.getPath().getSegments();
    }

    public Request getRequest() {
        return request;
    }

    public Response getResponse() {
        return response;
    }

    public Auth getAuth() {
        return auth;
    }

    public User getUser() {
        return auth.getUser();
    }

    @Override
    public String toString() {
        return request.getMethod() + " " + request.getPath().toString() + " auth=" + auth;
    }

    public String nextSegment() {
        return currentSegment < segments.length ? segments[currentSegment++] : null;
    }

    public boolean isGet() {
        return "GET".equals(request.getMethod());
    }

    public boolean isPost() {
        return "POST".equals(request.getMethod());
    }

    public boolean isPut() {
        return "PUT".equals(request.getMethod());
    }

    public boolean isDelete() {
        return "DELETE".equals(request.getMethod());
    }

    public int getInt(String param, int def) throws IOException {
        String s = request.getParameter(param);
        if (s == null || s.length() == 0) return def;
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return def;
        }
    }

    public boolean getBoolean(String param) throws IOException {
        return "true".equals(request.getParameter(param));
    }

    public void setCode(int code) throws IOException {
        setCode(code, null);
    }

    public void setCode(int code, String message) throws IOException {
        this.code = code;
        renderer.setCode(response, code, message);
    }

    public void setCode(int code, Object data) throws IOException {
        this.code = code;
        renderer.setCode(response, code, data);
    }

    public int getCode() {
        return code;
    }

    /**
     * Render o as json and write to the response.
     */
    public void setJson(Object o) throws IOException {
        renderer.json(response, o);
    }

    public Database getDatabase() {
        return database;
    }

    public void setDatabase(Database database) {
        this.database = database;
    }

    public Queue getQueue() {
        return queue;
    }

    public void setQueue(Queue queue) {
        this.queue = queue;
    }
}
