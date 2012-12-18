package io.qdb.server

import spock.lang.Specification
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder

class BaseSpec extends Specification {

    protected static final String SERVER_URL = "http://127.0.0.1:9554"

    protected GET(String path, String user = "admin", String password = "admin") {
        def headers = [:]
        if (user) {
            headers.Authorization = toBasicAuth(user, password);
        }
        return new JsonSlurper().parseText(new URL(SERVER_URL + path).getText([requestProperties: headers]))
    }

    private String toBasicAuth(String user, String password) {
        return "Basic " + (user + ":" + password).getBytes("UTF8").encodeBase64().toString()
    }

    protected POST(String path, Object data, String user = "admin", String password = "admin") {
        putOrPost("POST", path, data, user, password)
    }

    protected PUT(String path, Object data, String user = "admin", String password = "admin") {
        putOrPost("PUT", path, data, user, password)
    }

    private putOrPost(String method, String path, Object data, String user = "admin", String password = "admin") {
        String json = new JsonBuilder(data).toPrettyString()
        def url = new URL(SERVER_URL + path)
        HttpURLConnection con = url.openConnection() as HttpURLConnection
        con.doOutput = true
        con.requestMethod = method
        if (user) con.setRequestProperty("Authorization", toBasicAuth(user, password))
        con.setRequestProperty("Content-Type", "application/json")
        con.outputStream.write(json.getBytes("UTF8"))
        return new JsonSlurper().parseText(con.inputStream.text)
    }

}
