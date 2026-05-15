/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.concurrent.CompletableFuture;

import static io.kroxylicious.proxy.internal.ClientConnectionState.ClientActive;
import static io.kroxylicious.proxy.internal.ClientConnectionState.Closed;
import static io.kroxylicious.proxy.internal.ClientConnectionState.Draining;
import static io.kroxylicious.proxy.internal.ClientConnectionState.Forwarding;
import static io.kroxylicious.proxy.internal.ClientConnectionState.HaProxy;
import static io.kroxylicious.proxy.internal.ClientConnectionState.Startup;

/**
 * Root of a sealed class hierarchy representing the states of the {@link ClientConnectionStateMachine}.
 */
sealed interface ClientConnectionState permits
        Startup,
        ClientActive,
        HaProxy,
        Forwarding,
        Draining,
        Closed {

    /**
     * The statemachine has just been created.
     */
    record Startup() implements ClientConnectionState {
        public static final Startup STARTING_STATE = new Startup();

        public ClientActive toClientActive() {
            return new ClientActive();
        }
    }

    /**
     * The initial state, when a client has connected, but no messages
     * have been received yet.
     */
    record ClientActive() implements ClientConnectionState {

        /**
         * Transition to {@link HaProxy}, because a PROXY header has been received
         * @return The HaProxy state
         */
        public HaProxy toHaProxy() {
            return new HaProxy();
        }

        /**
         * Transition to {@link Forwarding}, because a client request has been received
         * and a backend connection is being initiated.
         * @return The Forwarding state
         */
        public Forwarding toForwarding() {
            return new Forwarding();
        }
    }

    /**
     * A PROXY protocol header has been received on the channel.
     * The connection metadata is captured in the {@link KafkaSession}'s
     * {@link io.kroxylicious.proxy.internal.net.HaProxyContext}.
     */
    record HaProxy()
            implements ClientConnectionState {

        /**
         * Transition to {@link Forwarding}, because a client request has been received
         * and a backend connection is being initiated.
         * @return The Forwarding state
         */
        public Forwarding toForwarding() {
            return new Forwarding();
        }
    }

    /**
     * A backend connection has been initiated. The {@link ClientConnectionStateMachine#progressionLatch}
     * gates client unblocking until both the transport subject is built and the backend
     * connection is active.
     */
    record Forwarding() implements ClientConnectionState {}

    /**
     * Connections are being drained. autoRead is disabled on the client channel,
     * but responses to in-flight requests continue flowing. When the in-flight count
     * reaches zero, the PCSM invokes {@code onDrained} — an externally-injected policy
     * (typically wired by {@code VirtualClusterLifecycle}) that decides what to do next
     * (cancel the timeout timer, complete the per-connection future, close the
     * connection with {@code DisconnectCause.DRAIN_COMPLETED}).
     * <p>
     * {@code closedFuture} completes when this connection has fully closed. Callers of
     * {@link ClientConnectionStateMachine#drain(java.time.Duration)} that arrive while the
     * connection is already draining chain their own promise to this future.
     */
    record Draining(Runnable onDrained, CompletableFuture<Void> closedFuture) implements ClientConnectionState {

    }

    /**
     * The final state, where there are no connections to either client or server
     */
    record Closed() implements ClientConnectionState {

    }

}
