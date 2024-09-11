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
import static io.kroxylicious.proxy.internal.ProxyChannelState.Closed;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Connecting;
import static io.kroxylicious.proxy.internal.ProxyChannelState.HaProxy;
import static io.kroxylicious.proxy.internal.ProxyChannelState.NegotiatingTls;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Forwarding;
import static io.kroxylicious.proxy.internal.ProxyChannelState.SelectingServer;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Start;

/**
 * The state machine for a single client's connection to a server.
 * Each state is represented by a subclass of
 * {@code State} which contains state-specific data.
 *
 * Using the regular expression metacharacter {@code ?} the possible sequence of transitions looks like this:
 * <pre>
 * {@link Start Start}                              // a client has connected
 * {@link HaProxy HaProxy}?                         // the client has sent an PROXY header
 * {@link ApiVersions ApiVersions}?                 // the client has send an ApiVersions request
 * {@link SelectingServer SelectingServer}          // delegating to {@link io.kroxylicious.proxy.filter.NetFilter}
 * {@link Connecting Connecting}                    // {@link io.kroxylicious.proxy.filter.NetFilter} has been called
 * {@link NegotiatingTls NegotiatingTls}?           // (If TLS configured) we've completed the TLS handshake
 * {@link Forwarding Forwarding}                    // Forwarding all KRPC requests to the server
 * {@link Closed Closed}                            // Closed both connections
 * </pre>
 */
@VisibleForTesting
sealed interface ProxyChannelState
        permits Start,
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

    default @NonNull List<Object> bufferedMsgs() {
        return List.of();
    }

    default Closed toClosed() {
        return new Closed();
    }

    /**
     * The initial state, when a client has connected, but no messages
     * have been received yey.
     * @param inboundCtx
     */
    record Start(ChannelHandlerContext inboundCtx) implements ProxyChannelState {

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
        public SelectingServer toSelectingServer() {
            return new SelectingServer(
                    inboundCtx,
                    null,
                    null,
                    null,
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
        public SelectingServer toSelectingServer() {
            return new SelectingServer(
                    inboundCtx,
                    haProxyMessage,
                    null,
                    null,
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
        public Connecting toConnecting() {
            return new Connecting(inboundCtx, haProxyMessage, clientSoftwareName,
                    clientSoftwareVersion, bufferedMsgs);
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

                      @NonNull List<Object> bufferedMsgs)
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
            sslHandler.handshakeFuture().addListener(x -> handler.onUpstreamSslOutcome(outboundCtx, x));
            return new NegotiatingTls(
                    inboundCtx,
                    haProxyMessage,
                    clientSoftwareName,
                    clientSoftwareVersion,
                    outboundCtx,
                    bufferedMsgs);
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
                    outboundCtx);
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
                          List<Object> bufferedMsgs)
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
                    outboundCtx);
        }

        public NegotiatingTls bufferMessage(Object msg) {
            this.bufferedMsgs.add(msg);
            return this;
        }
    }

    /**
     * There's a KRPC-capable channel to the server
     * @param inboundCtx The client content
     * @param haProxyMessage the info gleaned from the PROXY handshake, or null if the client didn't use the PROXY protocol
     * @param clientSoftwareName the name of the client library, or null if the client didn't send this.
     * @param clientSoftwareVersion the version of the client library, or null if the client didn't send this.
     * @param outboundCtx The server context
     */
    record Forwarding(
                          @NonNull ChannelHandlerContext inboundCtx,
                          @Nullable HAProxyMessage haProxyMessage,
                          @Nullable String clientSoftwareName,
                          @Nullable String clientSoftwareVersion,
                          @NonNull ChannelHandlerContext outboundCtx)
            implements ProxyChannelState {
        public Forwarding {
            Objects.requireNonNull(inboundCtx);
            Objects.requireNonNull(outboundCtx);
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
