/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

import java.util.Set;

import io.kroxylicious.identity.Identity;
import io.kroxylicious.identity.SingularPrincipals;

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
 * @param principals the set of identifiers associated with this subject.
 * @deprecated Use {@link io.kroxylicious.identity.Subject} instead. Will be removed at 1.0.
 */
@Deprecated(since = "0.24.0", forRemoval = true)
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
     * Creates a subject from the given principal set. Validates that non-empty subjects have exactly one {@link User} principal.
     * @param principals the principals
     */
    public Subject(Set<Principal> principals) {
        SingularPrincipals.validateUniqueness(principals);
        this.principals = Set.copyOf(principals);
        if (!this.principals.isEmpty() && uniquePrincipalOfType(User.class).isEmpty()) {
            throw new IllegalArgumentException("A subject with non-empty principals must have exactly one " + User.class.getName() + " principal.");
        }
    }

}
