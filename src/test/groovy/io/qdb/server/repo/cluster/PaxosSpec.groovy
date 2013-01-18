package io.qdb.server.repo.cluster

import spock.lang.Stepwise

@Stepwise
class PaxosSpec extends PaxosBase {

    def "Initial proposal sent"() {
        s1.start(["s1", "s2", "s3"] as Object[], "p1")

        expect:
        transport.sent.toString() == "[PREPARE n=11 v=p1 to s1, PREPARE n=11 v=p1 to s2, PREPARE n=11 v=p1 to s3]"
    }

}
