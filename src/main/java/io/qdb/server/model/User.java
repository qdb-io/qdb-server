package io.qdb.server.model;

/**
 * A user with permissions. The user's id is used as its username.
 */
public class User extends ModelObject {

    private String passwordHash;
    private boolean admin;
    private String[] databases;

    public User() {
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

    public void setAdmin(Boolean admin) {
        this.admin = admin;
    }

    public boolean doesPasswordMatch(String password) {
        // todo hash password
        return password.equals(passwordHash);
    }

    public String[] getDatabases() {
        return databases;
    }

    public void setDatabases(String[] databases) {
        this.databases = databases;
    }

    @Override
    public String toString() {
        return super.toString() + ":" + (admin ? "ADMIN" : "");
    }
}
