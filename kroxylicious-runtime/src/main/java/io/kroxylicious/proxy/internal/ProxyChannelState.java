/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.List;
import java.util.Objects;

import org.apache.kafka.common.message.ApiVersionsRequestData;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.haproxy.HAProxyMessage;

import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.proxy.internal.ProxyChannelState.ApiVersions;
import static io.kroxylicious.proxy.internal.ProxyChannelState.ClientActive;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Closed;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Connecting;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Forwarding;
import static io.kroxylicious.proxy.internal.ProxyChannelState.HaProxy;
import static io.kroxylicious.proxy.internal.ProxyChannelState.NegotiatingTls;
import static io.kroxylicious.proxy.internal.ProxyChannelState.SelectingServer;

/**
 * Root of a sealed class hierarchy representing the states of the {@link StateHolder}.
 */
@VisibleForTesting
sealed interface ProxyChannelState
        permits ClientActive,
        HaProxy,
        ApiVersions,
        SelectingServer,
        Connecting,
        NegotiatingTls,
        Forwarding,
        Closed {

    default @Nullable ChannelHandlerContext outboundCtx() {
        return null;
    }

    default @Nullable HostPort remote() {
        return null;
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
        public @NonNull HaProxy toHaProxy(HAProxyMessage haProxyMessage) {
            return new HaProxy(haProxyMessage);
        }

        /**
         * Transition to {@link ApiVersions}, because an ApiVersions request has been received
         * @return The ApiVersions state
         */
        @NonNull
        public ApiVersions toApiVersions(
                                         DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame) {
            // TODO check the format of the strings using a regex
            // Needed to reproduce the exact behaviour for how a broker handles this
            // see org.apache.kafka.common.requests.ApiVersionsRequest#isValid()
            var clientSoftwareName = apiVersionsFrame.body().clientSoftwareName();
            var clientSoftwareVersion = apiVersionsFrame.body().clientSoftwareVersion();
            return new ApiVersions(
                    null,
                    clientSoftwareName,
                    clientSoftwareVersion);
        }

        /**
         * Transition to {@link SelectingServer}, because some non-ApiVersions request has been received
         * @return The Connecting state
         */
        @NonNull
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

                   @NonNull HAProxyMessage haProxyMessage)
            implements ProxyChannelState {

        /**
         * Transition to {@link ApiVersions}, because an ApiVersions request has been received
         * @return The ApiVersions state
         */
        @NonNull
        public ApiVersions toApiVersions(DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame) {
            // TODO check the format of the strings using a regex
            // Needed to reproduce the exact behaviour for how a broker handles this
            // see org.apache.kafka.common.requests.ApiVersionsRequest#isValid()
            var clientSoftwareName = apiVersionsFrame.body().clientSoftwareName();
            var clientSoftwareVersion = apiVersionsFrame.body().clientSoftwareVersion();
            return new ApiVersions(
                    haProxyMessage,
                    clientSoftwareName,
                    clientSoftwareVersion);
        }

        /**
         * Transition to {@link SelectingServer}, because some non-ApiVersions request has been received
         * @return The Connecting state
         */
        @NonNull
        public SelectingServer toSelectingServer(@Nullable DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame) {
            return new SelectingServer(
                    haProxyMessage,
                    apiVersionsFrame == null ? null : apiVersionsFrame.body().clientSoftwareName(),
                    apiVersionsFrame == null ? null : apiVersionsFrame.body().clientSoftwareVersion());
        }
    }

    /**
     * The client has sent an ApiVersions request
     * @param haProxyMessage
     * @param clientSoftwareName
     * @param clientSoftwareVersion
     */
    record ApiVersions(@Nullable HAProxyMessage haProxyMessage,
                       @Nullable String clientSoftwareName, // optional in the protocol
                       @Nullable String clientSoftwareVersion // optional in the protocol
    ) implements ProxyChannelState {

        /**
         * Transition to {@link SelectingServer}, because some non-ApiVersions request has been received
         * @return The Connecting state
         */
        @NonNull
        public SelectingServer toSelectingServer() {
            return new SelectingServer(
                    haProxyMessage,
                    clientSoftwareName,
                    clientSoftwareVersion);
        }
    }

    /**
     * A channel to the server is now required, but
     * {@link io.kroxylicious.proxy.filter.NetFilter#selectServer(NetFilter.NetFilterContext)}
     * has not yet been called.
     * @param haProxyMessage
     * @param clientSoftwareName
     * @param clientSoftwareVersion
     */
    record SelectingServer(@Nullable HAProxyMessage haProxyMessage,
                           @Nullable String clientSoftwareName,
                           @Nullable String clientSoftwareVersion)
            implements ProxyChannelState {

        /**
         * Transition to {@link Connecting}, because the NetFilter
         * has invoked
         * {@link io.kroxylicious.proxy.filter.NetFilter.NetFilterContext#initiateConnect(HostPort, List)}.
         * @return The Connecting2 state
         */
        public Connecting toConnecting(@NonNull HostPort remote) {
            return new Connecting(haProxyMessage, clientSoftwareName,
                    clientSoftwareVersion, remote);
        }
    }

    /**
     * The NetFilter has determined the server to connect to,
     * but the channel to it is not yet active.
     * @param haProxyMessage
     * @param clientSoftwareName
     * @param clientSoftwareVersion
     */
    record Connecting(@Nullable HAProxyMessage haProxyMessage,
                      @Nullable String clientSoftwareName,
                      @Nullable String clientSoftwareVersion,
                      @NonNull HostPort remote)
            implements ProxyChannelState {

        /**
         * Transition to {@link NegotiatingTls}, in the case where TLS is configured.
         * @return The NegotiatingTls state
         */
        public @NonNull NegotiatingTls toNegotiatingTls(ChannelHandlerContext outboundCtx) {
            return new NegotiatingTls(
                    haProxyMessage,
                    clientSoftwareName,
                    clientSoftwareVersion,
                    outboundCtx,
                    remote);
        }

        /**
         * Transition to {@link Forwarding}, in the case where TLS is not configured.
         * @return The Forwarding state
         */
        @NonNull
        public Forwarding toForwarding(ChannelHandlerContext outboundCtx) {
            return new Forwarding(
                    haProxyMessage,
                    clientSoftwareName,
                    clientSoftwareVersion,
                    outboundCtx,
                    remote);
        }

    }

    /**
     * There's an active channel to the server, but TLS is configured and handshaking is in progress.
     * @param haProxyMessage the info gleaned from the PROXY handshake, or null if the client didn't use the PROXY protocol
     * @param clientSoftwareName the name of the client library, or null if the client didn't send this.
     * @param clientSoftwareVersion the version of the client library, or null if the client didn't send this.
     * @param outboundCtx The server context
     */
    record NegotiatingTls(
                          @Nullable HAProxyMessage haProxyMessage,
                          @Nullable String clientSoftwareName,
                          @Nullable String clientSoftwareVersion,
                          @NonNull ChannelHandlerContext outboundCtx,
                          @NonNull HostPort remote)
            implements ProxyChannelState {
        public NegotiatingTls {
            Objects.requireNonNull(outboundCtx);
        }

        /**
         * Transition to {@link Forwarding}
         * @return The Forwarding state
         */
        @NonNull
        public Forwarding toForwarding() {
            return new Forwarding(
                    haProxyMessage,
                    clientSoftwareName,
                    clientSoftwareVersion,
                    outboundCtx,
                    remote);
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
        @NonNull
        private final ChannelHandlerContext outboundCtx;

        private final HostPort remote;

        Forwarding(
                   @Nullable HAProxyMessage haProxyMessage,
                   @Nullable String clientSoftwareName,
                   @Nullable String clientSoftwareVersion,
                   @NonNull ChannelHandlerContext outboundCtx,
                   @NonNull HostPort remote) {
            Objects.requireNonNull(outboundCtx);
            this.haProxyMessage = haProxyMessage;
            this.clientSoftwareName = clientSoftwareName;
            this.clientSoftwareVersion = clientSoftwareVersion;
            this.outboundCtx = outboundCtx;
            this.remote = remote;
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
        @NonNull
        public ChannelHandlerContext outboundCtx() {
            return outboundCtx;
        }


        @NonNull
        public HostPort remote() {
            return remote;
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
                    Objects.equals(this.clientSoftwareVersion, that.clientSoftwareVersion) &&
                    Objects.equals(this.outboundCtx, that.outboundCtx) &&
                    Objects.equals(this.remote, that.remote);
        }

        @Override
        public int hashCode() {
            return Objects.hash(haProxyMessage, clientSoftwareName, clientSoftwareVersion, outboundCtx, remote);
        }

        @Override
        public String toString() {
            return "Forwarding[" +
                    "haProxyMessage=" + haProxyMessage + ", " +
                    "clientSoftwareName=" + clientSoftwareName + ", " +
                    "clientSoftwareVersion=" + clientSoftwareVersion + ", " +
                    "outboundCtx=" + outboundCtx + ']';
        }

    }

    /**
     * The final state, where there are no connections to either client or server
     */
    record Closed() implements ProxyChannelState {

    }

}
