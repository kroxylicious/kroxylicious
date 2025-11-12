/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import io.kroxylicious.proxy.authentication.User;

public class UserFactory implements PrincipalFactory<User> {
    @Override
    public User newPrincipal(String name) {
        return new User(name);
    }
}
