package io.qdb.server.model;

/**
 * A user with permissions.
 */
public class User extends ModelObject {

    private String email;
    private String passwordHash;
    private boolean admin;

    public User() {
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPasswordHash() {
        return passwordHash;
    }

    public void setPasswordHash(String passwordHash) {
        this.passwordHash = passwordHash;
    }

    public boolean isAdmin() {
        return admin;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    public boolean doesPasswordMatch(String password) {
        // todo hash password
        return password.equals(passwordHash);
    }

    @Override
    public String toString() {
        return super.toString() + ":" + email + ":" + (admin ? "ADMIN" : "");
    }
}
