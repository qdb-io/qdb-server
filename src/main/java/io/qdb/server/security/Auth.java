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
