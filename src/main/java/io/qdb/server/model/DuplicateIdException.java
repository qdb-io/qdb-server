package io.qdb.server.model;

/**
 * Thrown when creating new objects if there is already an object with the same id.
 */
public class DuplicateIdException extends ModelException {

    public DuplicateIdException(String message) {
        super(message);
    }

}
