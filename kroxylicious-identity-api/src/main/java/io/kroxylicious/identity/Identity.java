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
 * <p>Represents an actor in the system.
 * An identity is composed of a possibly-empty set of identifiers represented as {@link Principal} instances.
 * An anonymous actor is represented by an identity with an empty set of principals.</p>
 *
 * <p>This interface exists as a bridge type during the migration from
 * {@code io.kroxylicious.proxy.authentication.Subject} to {@link Subject}.
 * New code should use {@link Subject} directly.</p>
 *
 * @deprecated Use {@link Subject} instead. This interface will be removed in a future major version.
 */
@Deprecated(since = "0.23.0")
public interface Identity {

    /**
     * Returns the set of principals associated with this identity.
     * @return the principals
     */
    Set<? extends Principal> principals();

    /**
     * Returns whether this identity has no principals.
     * @return true if this identity has no principals
     */
    default boolean isAnonymous() {
        return principals().isEmpty();
    }

    /**
     * Returns the unique principal of the given type, if present.
     * @param uniquePrincipalType the principal type, which must be annotated with {@link SingularPrincipal}
     * @param <P> the principal type
     * @return the principal, or empty
     * @throws IllegalArgumentException if the type is not annotated with {@link SingularPrincipal}
     */
    default <P extends Principal> Optional<P> uniquePrincipalOfType(Class<P> uniquePrincipalType) {
        if (!uniquePrincipalType.isAnnotationPresent(SingularPrincipal.class)) {
            throw new IllegalArgumentException(uniquePrincipalType + " is not annotated with " + SingularPrincipal.class);
        }
        return principals().stream()
                .filter(uniquePrincipalType::isInstance)
                .map(uniquePrincipalType::cast)
                .findFirst();
    }

    /**
     * Returns all principals of the given type.
     * @param principalType the principal type
     * @param <P> the principal type
     * @return the matching principals
     */
    default <P extends Principal> Set<P> allPrincipalsOfType(Class<P> principalType) {
        return principals().stream()
                .filter(principalType::isInstance)
                .map(principalType::cast)
                .collect(Collectors.toSet());
    }
}
