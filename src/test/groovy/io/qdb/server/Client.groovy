package io.qdb.server

import groovy.json.JsonSlurper
import groovy.json.JsonBuilder

/**
 * Manages communication with a QDB server over HTTP for tests.
 */
class Client {

    String serverUrl

    static class Response {
        int code
        String text
        Object json
    }

    Client(String serverUrl) {
        this.serverUrl = serverUrl
    }

    Response GET(String path, String user = "admin", String password = "admin") {
        def url = new URL(serverUrl + path)
        HttpURLConnection con = url.openConnection() as HttpURLConnection
        if (user) con.setRequestProperty("Authorization", toBasicAuth(user, password))
        Response r = new Response()
        r.code = con.responseCode
        r.text = (r.code >= 200 && r.code < 300 ? con.inputStream : con.errorStream)?.getText("UTF8")
        if (r.text) r.json = new JsonSlurper().parseText(r.text)
        return r
    }

    private String toBasicAuth(String user, String password) {
        return "Basic " + (user + ":" + password).getBytes("UTF8").encodeBase64().toString()
    }

    Response POST(String path, Object data, String user = "admin", String password = "admin") {
        String json = new JsonBuilder(data).toPrettyString()
        putOrPost("POST", path, "application/json", json.getBytes("UTF8"), user, password)
    }

    Response POST(String path, String contentType, byte[] data, String user = "admin", String password = "admin") {
        putOrPost("POST", path, contentType, data, user, password)
    }

    Response PUT(String path, Object data, String user = "admin", String password = "admin") {
        String json = new JsonBuilder(data).toPrettyString()
        putOrPost("PUT", path, "application/json", json.getBytes("UTF8"), user, password)
    }

    private Response putOrPost(String method, String path, String contentType, byte[] data, String user, String password) {
        def url = new URL(serverUrl + path)
        HttpURLConnection con = url.openConnection() as HttpURLConnection
        con.doOutput = true
        con.requestMethod = method
        if (user) con.setRequestProperty("Authorization", toBasicAuth(user, password))
        con.setRequestProperty("Content-Type", contentType)
        con.outputStream.write(data)
        Response r = new Response()
        r.code = con.responseCode
        r.text = (r.code >= 200 && r.code < 300 ? con.inputStream : con.errorStream)?.getText("UTF8")
        if (r.text) r.json = new JsonSlurper().parseText(r.text)
        return r
    }

}
