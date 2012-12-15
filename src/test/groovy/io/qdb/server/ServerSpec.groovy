package io.qdb.server

class ServerSpec extends BaseSpec {

    def "Unauthenticated user can get basic status"() {
        def ans = GET("/")
        expect:
        ans.up == true
        ans.upSince != null
    }

    def "Authentication required for non-root urls"() {
        URL url = new URL(SERVER_URL + "/users")
        HttpURLConnection con = url.openConnection() as HttpURLConnection

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
