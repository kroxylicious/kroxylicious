/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

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
 * <li>information obtained about the client from a trusted source, such as lookup up role or group information from a directory.</li>
 * </ul>
 *
 * @param principals
 */
public record Subject(Set<Principal> principals) {

    private static final Subject ANONYMOUS = new Subject(Set.of());

    public static Subject anonymous() {
        return ANONYMOUS;
    }

    public Subject(Principal... principals) {
        this(Set.of(principals));
    }

    public Subject(Set<Principal> principals) {
        principals.stream()
                .collect(Collectors.groupingBy(Object::getClass))
                .forEach((principalClass, instances) -> {
                    if (principalClass.isAnnotationPresent(Unique.class) && instances.size() > 1) {
                        throw new IllegalArgumentException(
                                instances.size() + " principals of " + principalClass + " were found, but " + principalClass + " is annotated with " + Unique.class);
                    }

                });
        Optional<User> user = uniquePrincipalOfType(principals, User.class);
        if (!principals.isEmpty() && user.isEmpty()) {
            throw new IllegalArgumentException("A subject with non-empty principals must have exactly one " + User.class.getName() + " principal.");
        }
        this.principals = Set.copyOf(principals);
    }

    public <P extends Principal> Optional<P> uniquePrincipalOfType(Class<P> uniquePrincipalType) {
        return uniquePrincipalOfType(this.principals, uniquePrincipalType);
    }

    private static <P extends Principal> Optional<P> uniquePrincipalOfType(Set<Principal> principals, Class<P> uniquePrincipalType) {
        if (uniquePrincipalType.isAnnotationPresent(Unique.class)) {
            return principals.stream()
                    .filter(uniquePrincipalType::isInstance)
                    .map(uniquePrincipalType::cast)
                    .findFirst();
        }
        else {
            throw new IllegalArgumentException(uniquePrincipalType + " is not annotated with " + Unique.class);
        }
    }

    public <P extends Principal> Set<P> allPrincipalsOfType(Class<P> principalType) {
        return this.principals.stream()
                .filter(principalType::isInstance)
                .map(principalType::cast)
                .collect(Collectors.toSet());
    }

}
