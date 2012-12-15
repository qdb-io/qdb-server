package io.qdb.server

import spock.lang.Specification
import groovy.json.JsonSlurper

class BaseSpec extends Specification {

    protected static final String SERVER_URL = "http://127.0.0.1:9554"

    protected GET(String path) {
        return new JsonSlurper().parseText(new URL(SERVER_URL + path).text)
    }

}
