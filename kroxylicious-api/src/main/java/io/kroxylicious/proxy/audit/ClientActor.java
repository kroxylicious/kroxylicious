/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.audit;

import java.net.SocketAddress;
import java.util.Set;

import io.kroxylicious.proxy.authentication.Principal;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A TCP client. If the client is a Kafka client, then the proxy might also know the client principals.
 */
public non-sealed interface ClientActor extends Actor {
    SocketAddress srcAddr();

    String session();

    @Nullable
    Set<Principal> principals();
}
