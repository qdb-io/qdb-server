package io.qdb.server

class ServerSpec extends StandaloneBase {

    def "Unauthenticated user can get basic status"() {
        def ans = GET("/", null, null)

        expect:
        ans.code == 200
        ans.json.id != null
        ans.json.up == true
        ans.json.upSince != null
    }

    def "Authentication required for non-root urls"() {
        HttpURLConnection con = new URL(client.serverUrl + "/users").openConnection() as HttpURLConnection

        expect:
        con.responseCode == 401
        con.getHeaderField("WWW-Authenticate") == "basic realm=\"qdb\""
    }
}
