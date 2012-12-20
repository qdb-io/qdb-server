package io.qdb.server

class ServerSpec extends Base {

    def "Unauthenticated user can get basic status"() {
        def ans = GET("/", null, null)

        expect:
        ans.up == true
        ans.upSince != null
    }

    def "Authentication required for non-root urls"() {
        HttpURLConnection con = new URL(SERVER_URL + "/users").openConnection() as HttpURLConnection

        expect:
        con.responseCode == 401
        con.getHeaderField("WWW-Authenticate") == "basic realm=\"qdb\""
    }

//    def "length of Spock's and his friends' names"() {
//        expect:
//        name.size() == length
//
//        where:
//        name     | length
//        "Spock"  | 5
//        "Kirk"   | 4
//        "Scotty" | 6
//    }
}
