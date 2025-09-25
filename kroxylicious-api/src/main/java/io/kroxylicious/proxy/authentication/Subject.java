/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

import java.util.Set;

/**
 * <p>Represents an actor in the system.
 * Subjects are composed of a set of identifiers represented as {@link Principal} instances.</p>
 *
 * <p>The principals chosen depend on the calling code but in general might comprise the following:</p>
 * <li>information proven by a client, such as a SASL authorized id,</li>
 * <li>information known about the client, such as the remote peer's IP address,</li>
 * <li>information provided by the client, such as its Kafka client id</li>
 * <p>
 *     <strong>Security best practice says you should only trust information that's proven about the client.</strong>
 *     However, it is sometimes useful to have access to the other information for making authorization decisions.
 *     An example would be to deny a misbehaving client application identified by a
 *     (authorized id, client id)-pair from connecting to a cluster while the underlying problem is fixed.
 *     Such a decision narrowed access to a client based on untrusted information (the client id)
 *     to a client which would otherwise be allowed access (based on the SASL authorized id) on
 *     the premise that such a client is not malicious.
 *     Crucially, it does not expand access to clients based on that untrusted information.
 * </p>
 *
 * @param principals
 */
public record Subject(Set<Principal> principals) {

    public static final Subject ANONYMOUS = new Subject(Set.of());

    public Subject(Set<Principal> principals) {
        this.principals = Set.copyOf(principals);
    }

    public static Subject create(Set<Principal> principals) {
        if (principals.isEmpty()) {
            return ANONYMOUS;
        }
        return new Subject(principals);
    }
}
