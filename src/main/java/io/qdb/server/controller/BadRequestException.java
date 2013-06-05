package io.qdb.server.controller;

/**
 * Results in a 422 (default) or other status code being sent back to the client.
 */
public class BadRequestException extends RuntimeException {

    private int status;

    public BadRequestException(int status, String message) {
        super(message);
        this.status = status;
    }

    public BadRequestException(String message) {
        this(422, message);
    }

    public int getStatus() {
        return status;
    }
}
