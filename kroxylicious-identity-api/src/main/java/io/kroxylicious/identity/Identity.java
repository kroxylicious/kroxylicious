/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.identity;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>Bridge interface implemented by both {@link Subject} and the deprecated
 * {@code io.kroxylicious.proxy.authentication.Subject}, allowing either to be passed to APIs
 * (such as the Authorizer) that accept an identity during the transition to {@link Subject}.</p>
 *
 * @deprecated Use {@link Subject} directly. Will be removed at 1.0.
 */
@Deprecated(since = "0.24.0", forRemoval = true)
public interface Identity {

    /**
     * Returns the set of identifiers associated with this identity.
     * @return the principals, possibly empty
     */
    Set<? extends Principal> principals();

    /**
     * Returns the unique principal of the given type, if present.
     * @param type the principal type, which must be a {@linkplain SingularPrincipals#isSingular(Class) singular} type
     * @param <P> the principal type
     * @return the principal, or empty
     * @throws IllegalArgumentException if the type is not a singular principal type
     */
    default <P extends Principal> Optional<P> uniquePrincipalOfType(Class<P> type) {
        if (!SingularPrincipals.isSingular(type)) {
            throw new IllegalArgumentException(type + " is not a singular principal type.");
        }
        return principals().stream()
                .filter(type::isInstance)
                .map(type::cast)
                .findFirst();
    }

    /**
     * Returns all principals of the given type.
     * @param type the principal type
     * @param <P> the principal type
     * @return the matching principals
     */
    default <P extends Principal> Set<P> allPrincipalsOfType(Class<P> type) {
        return principals().stream()
                .filter(type::isInstance)
                .map(type::cast)
                .collect(Collectors.toSet());
    }

    /**
     * Returns whether this is the anonymous identity (no principals).
     * @return true if this identity has no principals
     */
    default boolean isAnonymous() {
        return principals().isEmpty();
    }

    /**
     * Returns the anonymous identity (no principals).
     * @return the anonymous identity
     */
    static Identity anonymous() {
        return Subject.anonymous();
    }
}
