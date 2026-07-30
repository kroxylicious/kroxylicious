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
 * Subjects are composed of a possibly-empty set of identifiers represented as {@link Principal} instances.
 * An anonymous actor is represented by a Subject with an empty set of principals.
 * As a convenience, {@link Subject#anonymous()} returns such a subject.
 * </p>
 *
 * <p>The principals included in a subject might comprise the following:</p>
 * <ul>
 * <li>information proven by a client, such as a SASL authorized id,</li>
 * <li>information known about the client, such as the remote peer's IP address,</li>
 * <li>information obtained about the client from a trusted source, such as looking up role or group information from a directory.</li>
 * </ul>
 *
 * @param principals the set of identifiers associated with this subject.
 */
@SuppressWarnings("deprecation")
public record Subject(Set<Principal> principals) implements Identity {

    private static final Subject ANONYMOUS = new Subject(Set.of());

    /**
     * Returns the anonymous subject (no principals).
     * @return the anonymous subject
     */
    public static Subject anonymous() {
        return ANONYMOUS;
    }

    /**
     * Creates a subject from the given principals.
     * @param principals the principals
     */
    public Subject(Principal... principals) {
        this(Set.of(principals));
    }

    /**
     * Creates a subject from the given principal set.
     * Validates that principal types annotated with {@link SingularPrincipal} have at most one instance.
     * @param principals the principals
     */
    public Subject(Set<Principal> principals) {
        principals.stream()
                .collect(Collectors.groupingBy(Object::getClass))
                .forEach((principalClass, instances) -> {
                    if (principalClass.isAnnotationPresent(SingularPrincipal.class) && instances.size() > 1) {
                        throw new IllegalArgumentException(
                                instances.size() + " principals of " + principalClass + " were found, but " + principalClass + " is annotated with "
                                        + SingularPrincipal.class);
                    }
                });
        this.principals = Set.copyOf(principals);
    }

    /**
     * Returns the unique principal of the given type, if present.
     * @param uniquePrincipalType the principal type, which must be annotated with {@link SingularPrincipal}
     * @param <P> the principal type
     * @return the principal, or empty
     * @throws IllegalArgumentException if the type is not annotated with {@link SingularPrincipal}
     */
    @Override
    public <P extends Principal> Optional<P> uniquePrincipalOfType(Class<P> uniquePrincipalType) {
        if (!uniquePrincipalType.isAnnotationPresent(SingularPrincipal.class)) {
            throw new IllegalArgumentException(uniquePrincipalType + " is not annotated with " + SingularPrincipal.class);
        }
        return principals.stream()
                .filter(uniquePrincipalType::isInstance)
                .map(uniquePrincipalType::cast)
                .findFirst();
    }

    /**
     * Returns whether this is the anonymous subject.
     * @return true if this subject has no principals
     */
    @Override
    public boolean isAnonymous() {
        return principals.isEmpty();
    }

    /**
     * Returns all principals of the given type.
     * @param principalType the principal type
     * @param <P> the principal type
     * @return the matching principals
     */
    @Override
    public <P extends Principal> Set<P> allPrincipalsOfType(Class<P> principalType) {
        return principals.stream()
                .filter(principalType::isInstance)
                .map(principalType::cast)
                .collect(Collectors.toSet());
    }
}
