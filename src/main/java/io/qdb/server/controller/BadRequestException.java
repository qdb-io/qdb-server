package io.qdb.server.controller;

/**
 * Results in a 400 being sent back to the client.
 */
public class BadRequestException extends RuntimeException {

    public BadRequestException(String message) {
        super(message);
    }

}
