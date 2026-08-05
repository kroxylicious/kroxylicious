/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

/**
 * A factory for {@link Principal} instances of a particular type.
 *
 * @param <P> The type of {@link Principal} created by this factory.
 */
public interface PrincipalFactory<P extends Principal> {
    /**
     * Creates a new principal with the given name.
     * @param name The name of the principal.
     * @return The new principal.
     */
    P newPrincipal(String name);
}
