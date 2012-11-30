package io.qdb.server.security;

/**
 * Looks up users.
 */
public interface UserRepository {

    /**
     * Find a user by email or return null if it does not exist.
     */
    public User findByEmail(String email);

}
