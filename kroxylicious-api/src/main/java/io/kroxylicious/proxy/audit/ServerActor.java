/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.audit;

import java.net.SocketAddress;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A TCP server. If the server is a Kafka server, then the proxy might also know the nodeId.
 */
public non-sealed interface ServerActor extends Actor {
    SocketAddress tgtAddr();

    String hostname();

    @Nullable
    Integer nodeId();
}
