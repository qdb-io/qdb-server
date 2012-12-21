package io.qdb.server

/**
 * Thrown when call returns something other than 200.
 */
class BadResponseCodeException extends IOException {

    int responseCode
    Object json

    BadResponseCodeException(String msg, int responseCode, Object json) {
        super(msg)
        this.responseCode = responseCode
        this.json = json
    }

    @Override
    String toString() {
        return super.toString() + (text ? "\n" + text : "")
    }
}
