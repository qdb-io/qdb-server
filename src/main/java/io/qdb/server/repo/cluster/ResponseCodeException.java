package io.qdb.server.repo.cluster;

import java.io.IOException;

/**
 * Thrown on unexpected HTTP response codes.
 */
public class ResponseCodeException extends IOException {

    public final int responseCode;
    public final String text;

    public ResponseCodeException(String message, int responseCode, String text) {
        super(message);
        this.responseCode = responseCode;
        this.text = text;
    }
}
