/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.identity;

import java.util.Set;

/**
 * <p>Represents an actor in the system.
 * Subjects are composed of a possibly-empty set of identifiers represented as {@link Principal} instances.
 * An anonymous actor is represented by a Subject with an empty set of principals.
 * As a convenience, {@link Subject#anonymous()} returns such a subject.</p>
 *
 * <p>The principals included in a subject might comprise the following:</p>
 * <ul>
 * <li>information proven by a client, such as a SASL authorized id,</li>
 * <li>information known about the client, such as the remote peer's IP address,</li>
 * <li>information obtained about the client from a trusted source, such as looked-up role or group information.</li>
 * </ul>
 *
 * @param principals the set of identifiers associated with this subject
 */
public record Subject(Set<? extends Principal> principals) implements Identity {

    private static final Subject ANONYMOUS = new Subject(Set.of());

    /**
     * Creates a subject from the given principal set. Validates that at most one principal of
     * each {@linkplain SingularPrincipal singular} type is present.
     * @param principals the principals
     */
    public Subject {
        SingularPrincipals.validateUniqueness(principals);
        principals = Set.copyOf(principals);
    }

    /**
     * Returns the anonymous subject (no principals).
     * @return the anonymous subject
     */
    public static Subject anonymous() {
        return ANONYMOUS;
    }
}
