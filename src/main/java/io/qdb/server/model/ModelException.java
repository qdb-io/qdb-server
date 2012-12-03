package io.qdb.server.model;

/**
 * Thrown for model validation errors and so on.
 */
public class ModelException extends RuntimeException {

    public ModelException(String message) {
        super(message);
    }
}
