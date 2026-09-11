/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

/**
 * A {@link PrincipalFactory} that creates {@link User} principals.
 */
public class UserFactory implements PrincipalFactory<User> {

    /**
     * Creates a new factory.
     */
    public UserFactory() {
        // Intentionally empty - declared only to carry documentation
    }

    @Override
    public User newPrincipal(String name) {
        return new User(name);
    }
}
