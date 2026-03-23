/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.kroxylicious.proxy.audit.ClientActor;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Subject;

import edu.umd.cs.findbugs.annotations.Nullable;

public record ClientActorImpl(SocketAddress srcAddr,
                              String session,
                              @Nullable Set<Principal> principals)
        implements ClientActor {
    public static ClientActorImpl of(SocketAddress addr,
                                     String session) {
        return new ClientActorImpl(Objects.requireNonNull(addr),
                Objects.requireNonNull(session),
                null);
    }

    public static ClientActorImpl of(SocketAddress addr,
                                     String session,
                                     @Nullable io.kroxylicious.proxy.authentication.Subject subject) {
        return new ClientActorImpl(Objects.requireNonNull(addr),
                Objects.requireNonNull(session),
                Optional.ofNullable(subject).map(Subject::principals).orElse(null));
    }
}
