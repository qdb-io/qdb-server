package io.qdb.server.databind;

import java.util.Map;

/**
 * Thrown on data binding errors.
 */
public class DataBindingException extends IllegalArgumentException {

    private Map<String, String> errors;

    public DataBindingException(Map<String, String> errors) {
        this.errors = errors;
    }

    @Override
    public String getMessage() {
        return errors.toString();
    }

    public Map<String, String> getErrors() {
        return errors;
    }
}
