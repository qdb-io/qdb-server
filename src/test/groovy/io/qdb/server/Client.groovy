package io.qdb.server

import groovy.json.JsonSlurper
import groovy.json.JsonBuilder

/**
 * Manages communication with a QDB server over HTTP for tests.
 */
class Client {

    String serverUrl

    Client(String serverUrl) {
        this.serverUrl = serverUrl
    }

    def GET(String path, String user = "admin", String password = "admin") {
        def url = new URL(serverUrl + path)
        HttpURLConnection con = url.openConnection() as HttpURLConnection
        if (user) con.setRequestProperty("Authorization", toBasicAuth(user, password))
        def rc = con.responseCode
        if (rc != 200) {
            def text = con.errorStream?.getText("UTF8")
            throw new BadResponseCodeException(
                    "Got ${rc} for GET ${url}",
                    rc, text, text ? new JsonSlurper().parseText(text) : null)
        } else {
            return new JsonSlurper().parseText(con.inputStream.text)
        }
    }

    private String toBasicAuth(String user, String password) {
        return "Basic " + (user + ":" + password).getBytes("UTF8").encodeBase64().toString()
    }

    def POST(String path, Object data, String user = "admin", String password = "admin") {
        putOrPost("POST", path, data, user, password)
    }

    def PUT(String path, Object data, String user = "admin", String password = "admin") {
        putOrPost("PUT", path, data, user, password)
    }

    private putOrPost(String method, String path, Object data, String user = "admin", String password = "admin") {
        String json = new JsonBuilder(data).toPrettyString()
        def url = new URL(serverUrl + path)
        HttpURLConnection con = url.openConnection() as HttpURLConnection
        con.doOutput = true
        con.requestMethod = method
        if (user) con.setRequestProperty("Authorization", toBasicAuth(user, password))
        con.setRequestProperty("Content-Type", "application/json")
        con.outputStream.write(json.getBytes("UTF8"))
        def rc = con.responseCode
        if (method == "PUT" && rc != 200 || method == "POST" && rc != 201) {
            def text = con.errorStream?.getText("UTF8")
            throw new BadResponseCodeException(
                    "Got ${rc} for ${method} ${url}",
                    rc, text, text ? new JsonSlurper().parseText(text) : null)
        } else {
            return new JsonSlurper().parseText(con.inputStream.text)
        }
    }

}
