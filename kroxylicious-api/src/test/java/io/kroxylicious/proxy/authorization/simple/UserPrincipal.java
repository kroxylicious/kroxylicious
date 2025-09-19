/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authorization.simple;

import io.kroxylicious.proxy.authorization.Principal;

public record UserPrincipal(String name) implements Principal {
}
