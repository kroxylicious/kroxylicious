/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl.foo;

import io.kroxylicious.authorizer.service.authorization.Principal;

public record X500Principal(String name) implements Principal {
    public X500Principal(javax.security.auth.x500.X500Principal x500Principal) {
        this(x500Principal.getName());
    }

}
