package io.qdb.server.model;

/**
 * Thrown when updating objects when the object has been modified by someone else since being read.
 */
public class OptLockException extends ModelException {

    public OptLockException(String message) {
        super(message);
    }

}
