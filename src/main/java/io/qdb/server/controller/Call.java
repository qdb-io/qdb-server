package io.qdb.server.controller;

import io.qdb.server.security.Auth;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;

/**
 * Encapsulates a call to the server. Includes the request, response and authentication information.
 */
public class Call {

    private final Request request;
    private final Response response;
    private final Auth auth;

    public Call(Request request, Response response, Auth auth) {
        this.request = request;
        this.response = response;
        this.auth = auth;
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

    @Override
    public String toString() {
        return request.getPath().toString() + " " + auth;
    }
}
