/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.common.message.ApiVersionsRequestData;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

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
 * <p>The state machine for a single client's connection to a server.
 * Each state is represented by an immutable subclass which contains state-specific data.</p>
 *
 * <pre>
 *   «start»
 *      │
 *      ↓ frontend.{@link KafkaProxyFrontendHandler#channelActive(ChannelHandlerContext) channelActive}
 *     {@link ClientActive ClientActive} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *  ╭───┤
 *  ↓   ↓ frontend.{@link KafkaProxyFrontendHandler#channelRead(ChannelHandlerContext, Object) channelRead} receives a PROXY header
 *  │  {@link HaProxy HaProxy} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *  ╰───┤
 *  ╭───┤
 *  ↓   ↓ frontend.{@link KafkaProxyFrontendHandler#channelRead(ChannelHandlerContext, Object) channelRead} receives an ApiVersions request
 *  │  {@link ApiVersions ApiVersions} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *  ╰───┤
 *      ↓ frontend.{@link KafkaProxyFrontendHandler#channelRead(ChannelHandlerContext, Object) channelRead} receives any other KRPC request
 *     {@link SelectingServer SelectingServer} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *      │
 *      ↓ netFiler.{@link NetFilter#selectServer(NetFilter.NetFilterContext) selectServer} calls frontend.{@link KafkaProxyFrontendHandler#initiateConnect(HostPort, List) initiateConnect}
 *     {@link Connecting Connecting} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *  ╭───┤
 *  ↓   ↓ backend.{@link KafkaProxyBackendHandler#channelActive(ChannelHandlerContext) channelActive} and TLS configured
 *  │  {@link NegotiatingTls NegotiatingTls} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *  ╰───┤
 *      ↓
 *     {@link Forwarding Forwarding} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *      │ backend.{@link KafkaProxyBackendHandler#channelInactive(ChannelHandlerContext) channelInactive}
 *      │ or frontend.{@link KafkaProxyFrontendHandler#channelInactive(ChannelHandlerContext) channelInactive}
 *      ↓
 *     {@link Closed Closed} ⇠╌╌╌╌ <b>error</b> ⇠╌╌╌╌
 * </pre>
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
    @NonNull
    ChannelHandlerContext inboundCtx();

    default @Nullable ChannelHandlerContext outboundCtx() {
        return null;
    }

    default @Nullable HostPort remote() {
        return null;
    }

    default @NonNull List<Object> bufferedMsgs() {
        return List.of();
    }

    /**
     * The initial state, when a client has connected, but no messages
     * have been received yet.
     * @param inboundCtx
     */
    record ClientActive(ChannelHandlerContext inboundCtx) implements ProxyChannelState {

        /**
         * Transition to {@link HaProxy}, because a PROXY header has been received
         * @return The HaProxy state
         */
        public @NonNull HaProxy toHaProxy(HAProxyMessage haProxyMessage) {
            return new HaProxy(inboundCtx, haProxyMessage);
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
                    inboundCtx,
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
                    inboundCtx,
                    null,
                    apiVersionsFrame == null ? null : apiVersionsFrame.body().clientSoftwareName(),
                    apiVersionsFrame == null ? null : apiVersionsFrame.body().clientSoftwareVersion(),
                    new ArrayList<>());
        }
    }

    /**
     * A PROXY protocol header has been received on the channel
     * @param inboundCtx The client context
     * @param haProxyMessage The information in the PROXY header
     */
    record HaProxy(
                   @NonNull ChannelHandlerContext inboundCtx,
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
                    inboundCtx,
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
                    inboundCtx,
                    haProxyMessage,
                    apiVersionsFrame == null ? null : apiVersionsFrame.body().clientSoftwareName(),
                    apiVersionsFrame == null ? null : apiVersionsFrame.body().clientSoftwareVersion(),
                    new ArrayList<>());
        }
    }

    /**
     * The client has sent an ApiVersions request
     * @param inboundCtx
     * @param haProxyMessage
     * @param clientSoftwareName
     * @param clientSoftwareVersion
     */
    record ApiVersions(@NonNull ChannelHandlerContext inboundCtx,
                       @Nullable HAProxyMessage haProxyMessage,
                       @Nullable String clientSoftwareName, // optional in the protocol
                       @Nullable String clientSoftwareVersion // optional in the protocol
    ) implements ProxyChannelState {
        public ApiVersions {
            Objects.requireNonNull(inboundCtx);
        }

        /**
         * Transition to {@link SelectingServer}, because some non-ApiVersions request has been received
         * @return The Connecting state
         */
        @NonNull
        public SelectingServer toSelectingServer() {
            return new SelectingServer(
                    inboundCtx,
                    haProxyMessage,
                    clientSoftwareName,
                    clientSoftwareVersion,
                    new ArrayList<>());
        }
    }

    /**
     * A channel to the server is now required, but
     * {@link io.kroxylicious.proxy.filter.NetFilter#selectServer(NetFilter.NetFilterContext)}
     * has not yet been called.
     * @param inboundCtx
     * @param haProxyMessage
     * @param clientSoftwareName
     * @param clientSoftwareVersion
     * @param bufferedMsgs
     */
    record SelectingServer(@NonNull ChannelHandlerContext inboundCtx,
                           @Nullable HAProxyMessage haProxyMessage,
                           @Nullable String clientSoftwareName,
                           @Nullable String clientSoftwareVersion,
                           @NonNull List<Object> bufferedMsgs)
            implements ProxyChannelState {
        public SelectingServer {
            Objects.requireNonNull(inboundCtx);
            Objects.requireNonNull(bufferedMsgs);
        }

        public SelectingServer bufferMessage(Object msg) {
            this.bufferedMsgs.add(msg);
            return this;
        }

        /**
         * Transition to {@link Connecting}, because the NetFilter
         * has invoked
         * {@link io.kroxylicious.proxy.filter.NetFilter.NetFilterContext#initiateConnect(HostPort, List)}.
         * @return The Connecting2 state
         */
        public Connecting toConnecting(@NonNull HostPort remote,
                                       @NonNull KafkaProxyBackendHandler backendHandler) {
            return new Connecting(inboundCtx, haProxyMessage, clientSoftwareName,
                    clientSoftwareVersion, bufferedMsgs, backendHandler, remote);
        }
    }

    /**
     * The NetFilter has determined the server to connect to,
     * but the channel to it is not yet active.
     * @param inboundCtx
     * @param haProxyMessage
     * @param clientSoftwareName
     * @param clientSoftwareVersion
     * @param bufferedMsgs
     */
    record Connecting(@NonNull ChannelHandlerContext inboundCtx,
                      @Nullable HAProxyMessage haProxyMessage,
                      @Nullable String clientSoftwareName,
                      @Nullable String clientSoftwareVersion,

                      @NonNull List<Object> bufferedMsgs,
                      @NonNull KafkaProxyBackendHandler backendHandler,
                      @NonNull HostPort remote)
            implements ProxyChannelState {

        public Connecting {
            Objects.requireNonNull(inboundCtx);
            Objects.requireNonNull(bufferedMsgs);
        }

        public Connecting bufferMessage(Object msg) {
            this.bufferedMsgs.add(msg);
            return this;
        }

        /**
         * Transition to {@link NegotiatingTls}, in the case where TLS is configured.
         * @return The NegotiatingTls state
         */
        public @NonNull NegotiatingTls toNegotiatingTls(KafkaProxyFrontendHandler handler,
                                                        ChannelHandlerContext outboundCtx,
                                                        SslContext sslContext) {
            final SslHandler sslHandler = outboundCtx.pipeline().get(SslHandler.class);
            sslHandler.handshakeFuture().addListener(handshakeFuture -> handler.onUpstreamSslOutcome(outboundCtx, handshakeFuture));
            return new NegotiatingTls(
                    inboundCtx,
                    haProxyMessage,
                    clientSoftwareName,
                    clientSoftwareVersion,
                    outboundCtx,
                    bufferedMsgs,
                    backendHandler,
                    remote);
        }

        /**
         * Transition to {@link Forwarding}, in the case where TLS is not configured.
         * @return The Forwarding state
         */
        @NonNull
        public Forwarding toForwarding(ChannelHandlerContext outboundCtx) {
            return new Forwarding(
                    inboundCtx,
                    haProxyMessage,
                    clientSoftwareName,
                    clientSoftwareVersion,
                    outboundCtx,
                    backendHandler,
                    remote);
        }

    }

    /**
     * There's an active channel to the server, but TLS is configured and handshaking is in progress.
     * @param inboundCtx The client content
     * @param haProxyMessage the info gleaned from the PROXY handshake, or null if the client didn't use the PROXY protocol
     * @param clientSoftwareName the name of the client library, or null if the client didn't send this.
     * @param clientSoftwareVersion the version of the client library, or null if the client didn't send this.
     * @param outboundCtx The server context
     */
    record NegotiatingTls(
                          @NonNull ChannelHandlerContext inboundCtx,
                          @Nullable HAProxyMessage haProxyMessage,
                          @Nullable String clientSoftwareName,
                          @Nullable String clientSoftwareVersion,
                          @NonNull ChannelHandlerContext outboundCtx,
                          List<Object> bufferedMsgs,
                          @NonNull KafkaProxyBackendHandler backendHandler,
                          @NonNull HostPort remote)
            implements ProxyChannelState {
        public NegotiatingTls {
            Objects.requireNonNull(inboundCtx);
            Objects.requireNonNull(outboundCtx);
        }

        /**
         * Transition to {@link Forwarding}
         * @return The Forwarding state
         */
        @NonNull
        public Forwarding toForwarding() {
            return new Forwarding(
                    inboundCtx,
                    haProxyMessage,
                    clientSoftwareName,
                    clientSoftwareVersion,
                    outboundCtx,
                    backendHandler,
                    remote);
        }

        public NegotiatingTls bufferMessage(Object msg) {
            this.bufferedMsgs.add(msg);
            return this;
        }
    }

    /**
     * There's a KRPC-capable channel to the server
     */
    final class Forwarding
            implements ProxyChannelState {
        @NonNull
        private final ChannelHandlerContext inboundCtx;
        @Nullable
        private final HAProxyMessage haProxyMessage;
        @Nullable
        private final String clientSoftwareName;
        @Nullable
        private final String clientSoftwareVersion;
        @NonNull
        private final ChannelHandlerContext outboundCtx;
        @NonNull
        private final KafkaProxyBackendHandler backendHandler;
        private final HostPort remote;

        Forwarding(
                   @NonNull ChannelHandlerContext inboundCtx,
                   @Nullable HAProxyMessage haProxyMessage,
                   @Nullable String clientSoftwareName,
                   @Nullable String clientSoftwareVersion,
                   @NonNull ChannelHandlerContext outboundCtx,
                   @NonNull KafkaProxyBackendHandler backendHandler,
                   @NonNull HostPort remote) {
            Objects.requireNonNull(inboundCtx);
            Objects.requireNonNull(outboundCtx);
            this.inboundCtx = inboundCtx;
            this.haProxyMessage = haProxyMessage;
            this.clientSoftwareName = clientSoftwareName;
            this.clientSoftwareVersion = clientSoftwareVersion;
            this.outboundCtx = outboundCtx;
            this.backendHandler = backendHandler;
            this.remote = remote;
        }

        @Override
        @NonNull
        public ChannelHandlerContext inboundCtx() {
            return inboundCtx;
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
        public KafkaProxyBackendHandler backendHandler() {
            return backendHandler;
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
            return Objects.equals(this.inboundCtx, that.inboundCtx) &&
                    Objects.equals(this.haProxyMessage, that.haProxyMessage) &&
                    Objects.equals(this.clientSoftwareName, that.clientSoftwareName) &&
                    Objects.equals(this.clientSoftwareVersion, that.clientSoftwareVersion) &&
                    Objects.equals(this.outboundCtx, that.outboundCtx) &&
                    Objects.equals(this.backendHandler, that.backendHandler) &&
                    Objects.equals(this.remote, that.remote);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inboundCtx, haProxyMessage, clientSoftwareName, clientSoftwareVersion, outboundCtx, backendHandler, remote);
        }

        @Override
        public String toString() {
            return "Forwarding[" +
                    "inboundCtx=" + inboundCtx + ", " +
                    "haProxyMessage=" + haProxyMessage + ", " +
                    "clientSoftwareName=" + clientSoftwareName + ", " +
                    "clientSoftwareVersion=" + clientSoftwareVersion + ", " +
                    "outboundCtx=" + outboundCtx + ", " +
                    "backendHandler=" + backendHandler + ']';
        }

    }

    /**
     * The final state, where there are no connections to either client or server
     */
    record Closed() implements ProxyChannelState {
        @NonNull
        @Override
        public ChannelHandlerContext inboundCtx() {
            return null;
        }
    }

}
