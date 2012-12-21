package io.qdb.server.model;

import io.qdb.server.security.PasswordTools;

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

    public void setPassword(String password) {
        passwordHash = password == null ? null : PasswordTools.hashPassword(password);
    }

    public boolean isAdmin() {
        return admin;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    public boolean doesPasswordMatch(String password) {
        return PasswordTools.checkPassword(password, passwordHash);
    }

    public String[] getDatabases() {
        return databases;
    }

    public void setDatabases(String[] databases) {
        this.databases = databases;
    }

    public boolean canReadDatabase(String database) {
        if (databases != null) {
            for (String db : databases) {
                if (db.equals(database)) return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return super.toString() + ":" + (admin ? "ADMIN" : "");
    }
}
