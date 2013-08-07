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

import io.qdb.server.databind.DateTimeParser;
import io.qdb.server.model.Database;
import io.qdb.server.model.Queue;
import io.qdb.server.model.User;
import io.qdb.server.security.Auth;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

/**
 * Encapsulates a call to the server. Includes the request, response and authentication information. Adds
 * convenience methods to render responses.
 */
public class Call {

    private final Request request;
    private final Response response;
    private final String[] segments;
    private final Renderer renderer;

    private int currentSegment;
    private int code = 200;

    private Auth auth;
    private Database database;
    private Queue queue;

    public Call(Request request, Response response, Renderer renderer) {
        this.request = request;
        this.response = response;
        this.renderer = renderer;
        this.segments = request.getPath().getSegments();
    }

    public String[] getSegments() {
        return segments;
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

    public void setAuth(Auth auth) {
        this.auth = auth;
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

    public long getLong(String param, long def) throws IOException {
        String s = request.getParameter(param);
        if (s == null || s.length() == 0) return def;
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return def;
        }
    }

    public String getString(String param, String def) throws IOException {
        String s = request.getParameter(param);
        return s == null ? def : s;
    }

    public byte[] getUTF8Bytes(String param, String def) throws IOException {
        String s = request.getParameter(param);
        return s == null ? def == null ? null : def.getBytes("UTF8") : s.getBytes("UTF8");
    }

    public boolean getBoolean(String param) throws IOException {
        return "true".equals(request.getParameter(param));
    }

    public Date getDate(String param) throws IOException {
        String s = request.getParameter(param);
        try {
            return s == null ? null : DateTimeParser.INSTANCE.parse(s);
        } catch (ParseException e) {
            throw new IllegalArgumentException(param  + "[" + s + "] is invalid: " + e.getMessage());
        }
    }

    public long getTimestamp(String param) throws IOException {
        Date d = getDate(param);
        return d == null ? -1 : d.getTime();
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
        renderer.setCode(response, code, data, getBoolean("borg"));
    }

    public int getCode() {
        return code;
    }

    public void setText(int code, String message) throws IOException {
        this.code = code;
        renderer.setCode(response, code, message);
    }

    /**
     * Render o as json and write to the response.
     */
    public void setJson(Object o) throws IOException {
        renderer.json(response, o, getBoolean("borg"));
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
