/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Objects;

import org.apache.kafka.common.message.ApiVersionsRequestData;

import io.netty.handler.codec.haproxy.HAProxyMessage;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.proxy.internal.ProxyChannelState.ClientActive;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Closed;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Connecting;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Forwarding;
import static io.kroxylicious.proxy.internal.ProxyChannelState.HaProxy;
import static io.kroxylicious.proxy.internal.ProxyChannelState.SelectingServer;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Startup;

/**
 * Root of a sealed class hierarchy representing the states of the {@link ProxyChannelStateMachine}.
 */
sealed interface ProxyChannelState permits
        Startup,
        ClientActive,
        HaProxy,
        SelectingServer,
        Connecting,
        Forwarding,
        Closed {

    /**
     * The statemachine has just been created.
     */
    record Startup() implements ProxyChannelState {
        public static final Startup STARTING_STATE = new Startup();

        public ClientActive toClientActive() {
            return new ClientActive();
        }
    }

    /**
     * The initial state, when a client has connected, but no messages
     * have been received yet.
     */
    record ClientActive() implements ProxyChannelState {

        /**
         * Transition to {@link HaProxy}, because a PROXY header has been received
         * @return The HaProxy state
         */
        public HaProxy toHaProxy(HAProxyMessage haProxyMessage) {
            return new HaProxy(haProxyMessage);
        }

        /**
         * Transition to {@link SelectingServer}, because some non-ApiVersions request has been received
         * @return The Connecting state
         */
        public SelectingServer toSelectingServer(@Nullable DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame) {
            return new SelectingServer(
                    null,
                    apiVersionsFrame == null ? null : apiVersionsFrame.body().clientSoftwareName(),
                    apiVersionsFrame == null ? null : apiVersionsFrame.body().clientSoftwareVersion());
        }
    }

    /**
     * A PROXY protocol header has been received on the channel
     * @param haProxyMessage The information in the PROXY header
     */
    record HaProxy(
                   HAProxyMessage haProxyMessage)
            implements ProxyChannelState {

        /**
         * Transition to {@link SelectingServer}, because some non-ApiVersions request has been received
         * @return The Connecting state
         */
        public SelectingServer toSelectingServer(@Nullable DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame) {
            return new SelectingServer(
                    haProxyMessage,
                    apiVersionsFrame == null ? null : apiVersionsFrame.body().clientSoftwareName(),
                    apiVersionsFrame == null ? null : apiVersionsFrame.body().clientSoftwareVersion());
        }
    }

    /**
     * A channel to the server is now required.
     * @param haProxyMessage
     * @param clientSoftwareName
     * @param clientSoftwareVersion
     */
    record SelectingServer(@Nullable HAProxyMessage haProxyMessage,
                           @Nullable String clientSoftwareName,
                           @Nullable String clientSoftwareVersion)
            implements ProxyChannelState {

        /**
         * Transition to {@link Connecting}
         * @return The Connecting state
         */
        public Connecting toConnecting(HostPort remote) {
            return new Connecting(haProxyMessage, clientSoftwareName,
                    clientSoftwareVersion, remote);
        }
    }

    /**
     * The connection has started but the channel to it is not yet active.
     *
     * @param haProxyMessage
     * @param clientSoftwareName
     * @param clientSoftwareVersion
     * @param remote
     */
    record Connecting(@Nullable HAProxyMessage haProxyMessage,
                      @Nullable String clientSoftwareName,
                      @Nullable String clientSoftwareVersion,
                      HostPort remote)
            implements ProxyChannelState {

        /**
         * Transition to {@link Forwarding}
         * @return The Forwarding state
         */
        public Forwarding toForwarding() {
            return new Forwarding(
                    haProxyMessage,
                    clientSoftwareName,
                    clientSoftwareVersion);
        }

    }

    /**
     * There's a KRPC-capable channel to the server
     */
    final class Forwarding
            implements ProxyChannelState {

        @Nullable
        private final HAProxyMessage haProxyMessage;
        @Nullable
        private final String clientSoftwareName;
        @Nullable
        private final String clientSoftwareVersion;

        Forwarding(
                   @Nullable HAProxyMessage haProxyMessage,
                   @Nullable String clientSoftwareName,
                   @Nullable String clientSoftwareVersion) {
            this.haProxyMessage = haProxyMessage;
            this.clientSoftwareName = clientSoftwareName;
            this.clientSoftwareVersion = clientSoftwareVersion;
        }

        @Nullable
        public HAProxyMessage haProxyMessage() {
            return haProxyMessage;
        }

        @Nullable
        public String clientSoftwareName() {
            return clientSoftwareName;
        }

        @Nullable
        public String clientSoftwareVersion() {
            return clientSoftwareVersion;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != this.getClass()) {
                return false;
            }
            var that = (Forwarding) obj;
            return Objects.equals(this.haProxyMessage, that.haProxyMessage) &&
                    Objects.equals(this.clientSoftwareName, that.clientSoftwareName) &&
                    Objects.equals(this.clientSoftwareVersion, that.clientSoftwareVersion);
        }

        @Override
        public int hashCode() {
            return Objects.hash(haProxyMessage, clientSoftwareName, clientSoftwareVersion);
        }

        @Override
        public String toString() {
            return "Forwarding[" +
                    "haProxyMessage=" + haProxyMessage + ", " +
                    "clientSoftwareName=" + clientSoftwareName + ", " +
                    "clientSoftwareVersion=" + clientSoftwareVersion + ']';
        }

    }

    /**
     * The final state, where there are no connections to either client or server
     */
    record Closed() implements ProxyChannelState {

    }

}
