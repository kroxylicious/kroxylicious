/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import io.kroxylicious.proxy.service.HostPort;

import static io.kroxylicious.proxy.internal.ServerConnectionState.Active;
import static io.kroxylicious.proxy.internal.ServerConnectionState.Closed;
import static io.kroxylicious.proxy.internal.ServerConnectionState.Connecting;

/**
 * States of the {@link ServerConnectionStateMachine}.
 */
sealed interface ServerConnectionState permits
        Connecting,
        Active,
        Closed {

    /**
     * TCP (and optionally TLS) connection to the broker is in progress.
     * Requests are buffered until the connection becomes active.
     *
     * @param remote the broker address being connected to
     */
    record Connecting(HostPort remote) implements ServerConnectionState {

        public Active toActive() {
            return new Active();
        }
    }

    /**
     * The connection is established and ready for KRPC.
     */
    record Active() implements ServerConnectionState {}

    /**
     * Terminal state. The backend channel is closed.
     */
    record Closed() implements ServerConnectionState {}
}
