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
import static io.kroxylicious.proxy.internal.ServerConnectionState.Draining;

/**
 * States of the {@link ServerConnectionStateMachine}.
 */
sealed interface ServerConnectionState permits
        Connecting,
        Active,
        Draining,
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
    record Active() implements ServerConnectionState {

        public Draining toDraining(Runnable onDrained) {
            return new Draining(onDrained);
        }
    }

    /**
     * No new requests will be sent. In-flight responses are still completing.
     * When the in-flight count reaches zero, {@code onDrained} is invoked.
     *
     * @param onDrained callback invoked when all in-flight responses have been received
     */
    record Draining(Runnable onDrained) implements ServerConnectionState {}

    /**
     * Terminal state. The backend channel is closed.
     */
    record Closed() implements ServerConnectionState {}
}
