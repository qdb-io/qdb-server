package io.qdb.server.security;

/**
 * A user with permissions.
 */
public class User {

    private final String id;
    private final String email;
    private final String passwordHash;
    private final boolean admin;

    public User(String id, String email, String passwordHash, boolean admin) {
        this.id = id;
        this.email = email;
        this.passwordHash = passwordHash;
        this.admin = admin;
    }

    public String getId() {
        return id;
    }

    public String getEmail() {
        return email;
    }

    public boolean isAdmin() {
        return admin;
    }

    public boolean doesPasswordMatch(String password) {
        // todo hash password
        return password.equals(passwordHash);
    }

    @Override
    public String toString() {
        return id + ":" + email + ":" + (admin ? "ADMIN" : "");
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof User && id.equals(((User)o).id);
    }
}
