package io.qdb.server.security;

import io.qdb.server.model.User;

/**
 * An authenticated user.
 */
public class Auth {

    private final User user;
    private final String identifiedBy;

    public Auth(User user, String identifiedBy) {
        this.user = user;
        this.identifiedBy = identifiedBy;
    }

    public Auth() {
        user = null;
        identifiedBy = "n/a";
    }

    public boolean isAnonymous() {
        return user == null;
    }

    /**
     * This is null for anonymous users.
     */
    public User getUser() {
        return user;
    }

    public String getIdentifiedBy() {
        return identifiedBy;
    }

    @Override
    public String toString() {
        if (isAnonymous()) return "ANONYMOUS";
        return user + " identified by " + identifiedBy;
    }
}
