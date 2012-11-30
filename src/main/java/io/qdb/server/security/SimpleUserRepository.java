package io.qdb.server.security;

import javax.inject.Singleton;

/**
 * Simple repo for development. Later this info will be stored in ZooKeeper.
 */
@Singleton
public class SimpleUserRepository implements UserRepository {

    @Override
    public User findByEmail(String email) {
        if (david.getEmail().equals(email)) {
            return david;
        }
        return null;
    }

    private static final User david = new User("1", "david.tinker@gmail.com", "secret", true);
}
