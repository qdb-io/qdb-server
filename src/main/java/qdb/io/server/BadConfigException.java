package qdb.io.server;

/**
 * Thrown for configuration problems.
 */
public class BadConfigException extends RuntimeException {

    public BadConfigException() {
    }

    public BadConfigException(String message) {
        super(message);
    }

    public BadConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public BadConfigException(Throwable cause) {
        super(cause);
    }
}
