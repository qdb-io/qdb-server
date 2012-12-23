package io.qdb.server

/**
 * Thrown when call returns something other than 200.
 */
class BadResponseCodeException extends IOException {

    int responseCode
    Object json
    String text

    BadResponseCodeException(String msg, int responseCode, String text, Object json) {
        super(msg)
        this.responseCode = responseCode
        this.json = json
        this.text = text;
    }

    @Override
    String toString() {
        return super.toString() + (text ? "\n" + text : "")
    }
}
