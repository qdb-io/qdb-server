/*
 * Copyright 2013 David Tinker
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.qdb.server.model;

import io.qdb.server.security.PasswordTools;

import java.util.Arrays;

/**
 * A user with permissions. The user's id is used as its username.
 */
public class User extends ModelObject {

    private String passwordHash;
    private boolean admin;
    private String[] databases;

    public User() {
    }

    public User(String id) {
        setId(id);
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
        return passwordHash != null && PasswordTools.checkPassword(password, passwordHash);
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

    public User deepCopy() {
        User u = (User)clone();
        if (databases != null) u.databases = databases.clone();
        return u;
    }
}
