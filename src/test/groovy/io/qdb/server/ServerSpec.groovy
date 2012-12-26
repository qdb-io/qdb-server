package io.qdb.server

class ServerSpec extends Base {

    def "Unauthenticated user can get basic status"() {
        def ans = GET("/", null, null)

        expect:
        ans.id != null
        ans.up == true
        ans.upSince != null
    }

    def "Authentication required for non-root urls"() {
        HttpURLConnection con = new URL(client.serverUrl + "/users").openConnection() as HttpURLConnection

        expect:
        con.responseCode == 401
        con.getHeaderField("WWW-Authenticate") == "basic realm=\"qdb\""
    }
}
