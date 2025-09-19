/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authorization.simple;

import io.kroxylicious.proxy.authorization.Principal;

record RolePrincipal(String name) implements Principal {
}
