/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import edu.umd.cs.findbugs.annotations.NonNull;

import edu.umd.cs.findbugs.annotations.Nullable;

import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.haproxy.HAProxyMessage;

import io.netty.handler.ssl.SslContext;

import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;

import java.util.List;
import java.util.Objects;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * <p>The state machine for a single client's connection to a server.
 * The "session state" is held in the {@link #state} field and is represented by an immutable
 * subclass of {@link ProxyChannelState} which contains state-specific data.
 * Events which cause state transitions are represented by the {@code on*()} family of methods.
 * Depending on the transition the frontend or backend handlers may get notified via one if their
 * {@code in*()} methods.
 * </p>
 *
 * <pre>
 *   «start»
 *      │
 *      ↓ frontend.{@link KafkaProxyFrontendHandler#channelActive(ChannelHandlerContext) channelActive}
 *     {@link ProxyChannelState.ClientActive ClientActive} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *  ╭───┤
 *  ↓   ↓ frontend.{@link KafkaProxyFrontendHandler#channelRead(ChannelHandlerContext, Object) channelRead} receives a PROXY header
 *  │  {@link ProxyChannelState.HaProxy HaProxy} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *  ╰───┤
 *  ╭───┤
 *  ↓   ↓ frontend.{@link KafkaProxyFrontendHandler#channelRead(ChannelHandlerContext, Object) channelRead} receives an ApiVersions request
 *  │  {@link ProxyChannelState.ApiVersions ApiVersions} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *  ╰───┤
 *      ↓ frontend.{@link KafkaProxyFrontendHandler#channelRead(ChannelHandlerContext, Object) channelRead} receives any other KRPC request
 *     {@link ProxyChannelState.SelectingServer SelectingServer} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *      │
 *      ↓ netFiler.{@link NetFilter#selectServer(NetFilter.NetFilterContext) selectServer} calls frontend.{@link KafkaProxyFrontendHandler#initiateConnect(HostPort, List) initiateConnect}
 *     {@link ProxyChannelState.Connecting Connecting} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *  ╭───┤
 *  ↓   ↓ backend.{@link KafkaProxyBackendHandler#channelActive(ChannelHandlerContext) channelActive} and TLS configured
 *  │  {@link ProxyChannelState.NegotiatingTls NegotiatingTls} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *  ╰───┤
 *      ↓
 *     {@link ProxyChannelState.Forwarding Forwarding} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *      │ backend.{@link KafkaProxyBackendHandler#channelInactive(ChannelHandlerContext) channelInactive}
 *      │ or frontend.{@link KafkaProxyFrontendHandler#channelInactive(ChannelHandlerContext) channelInactive}
 *      ↓
 *     {@link ProxyChannelState.Closed Closed} ⇠╌╌╌╌ <b>error</b> ⇠╌╌╌╌
 * </pre>
 *
 * <p>In addition to the "session state" this class also manages a second state machine for
 * handling TCP backpressure via the {@link #clientReadsBlocked} and {@link #serverReadsBlocked} field:</p>
 *
 * <pre>
 *     bothBlocked ←────────────────→ serverBlocked
 *         ↑                                ↑
 *         │                                │
 *         ↓                                ↓
 *    clientBlocked ←───────────────→ neitherBlocked
 * </pre>
 * <p>Note that this backpressure state machine is not related to the
 * session state machine: in general backpressure could happen in
 * several of the session states.</p>
 */
public class StateHolder {
    private static final Logger LOGGER = getLogger(StateHolder.class);

    @Nullable ProxyChannelState state;
    /* Track autoread states here, because the netty autoread flag is volatile =>
     expensive to set in every call to channelRead */
    boolean serverReadsBlocked;
    boolean clientReadsBlocked;
    /**
     * The frontend handler. Non-null if we got as far as ClientActive.
     */
    @Nullable KafkaProxyFrontendHandler frontendHandler;
    /**
     * The backend handler. Non-null if {@link #onNetFilterInitiateConnect(HostPort, List, VirtualCluster, NetFilter)}
     * has been called
     */
    @Nullable KafkaProxyBackendHandler backendHandler;

    public StateHolder() {
    }

    @VisibleForTesting
    ProxyChannelState state() {
        return state;
    }

    void setState(@NonNull ProxyChannelState state) {
        LOGGER.trace("{} transitioning to {}", this, state);
        this.state = state;
    }

    void onClientUnwritable() {
        if (!serverReadsBlocked) {
            serverReadsBlocked = true;
            Objects.requireNonNull(backendHandler).blockServerReads();
        }
    }

    void onClientWritable() {
        if (serverReadsBlocked) {
            serverReadsBlocked = false;
            Objects.requireNonNull(backendHandler).unblockServerReads();
        }
    }

    /**
     * The channel to the server is no longer writable
     */
    void onServerUnwritable() {
        if (!clientReadsBlocked) {
            clientReadsBlocked = true;
            Objects.requireNonNull(frontendHandler).blockClientReads();
        }
    }

    /**
     * The channel to the server is now writable
     */
    void onServerWritable() {
        if (clientReadsBlocked) {
            clientReadsBlocked = false;
            Objects.requireNonNull(frontendHandler).unblockClientReads();
        }
    }

    @VisibleForTesting
    void onClientActive(@NonNull KafkaProxyFrontendHandler frontendHandler) {
        if (this.state == null) {
            this.frontendHandler = frontendHandler;
            toClientActive(new ProxyChannelState.ClientActive(), frontendHandler);
        }
        else {
            illegalState("Client activation while not in the start state");
        }

    }

    private void toClientActive(
            @NonNull ProxyChannelState.ClientActive clientActive,
            @NonNull KafkaProxyFrontendHandler frontendHandler) {
        setState(clientActive);
        frontendHandler.inClientActive();
    }

    void onNetFilterInitiateConnect(
            @NonNull HostPort remote,
            @NonNull List<FilterAndInvoker> filters,
            VirtualCluster virtualCluster,
            NetFilter netFilter) {
        if (state instanceof ProxyChannelState.SelectingServer selectingServerState) {
            toConnecting(selectingServerState.toConnecting(remote), remote, filters, virtualCluster);
        }
        else {
            // TODO why do we do the state change here, rather than
            // throwing ISE like the other methods overridden from
            // NFC, then just catch ISE in selectServer?
            String msg = "NetFilter called NetFilterContext.initiateConnect() more than once";
            illegalState(msg + " : filter='" + netFilter + "'");
        }
    }

    private void toConnecting(ProxyChannelState.Connecting connecting,
                              @NonNull HostPort remote,
                              @NonNull List<FilterAndInvoker> filters,
                              VirtualCluster virtualCluster) {
        setState(connecting);
        backendHandler = new KafkaProxyBackendHandler(this, virtualCluster);
        Objects.requireNonNull(frontendHandler).inConnecting(remote, filters, backendHandler);
    }

    void onServerActive(ChannelHandlerContext serverCtx,
                        @Nullable SslContext sslContext) {
        if (state() instanceof ProxyChannelState.Connecting connectedState) {
            if (sslContext != null) {
                toNegotiatingTls(connectedState.toNegotiatingTls(serverCtx));
            }
            else {
                toForwarding(connectedState.toForwarding(serverCtx));
            }
        }
        else {
            illegalState("Server became active while not in the connecting state");
        }
    }

    private void toNegotiatingTls(ProxyChannelState.NegotiatingTls negotiatingTls) {
        setState(negotiatingTls);
    }

    private void toForwarding(ProxyChannelState.Forwarding forwarding) {
        setState(forwarding);
        Objects.requireNonNull(frontendHandler).inForwarding();
    }

    public void onServerTlsHandshakeCompletion(SslHandshakeCompletionEvent sslEvt) {
        if (state instanceof ProxyChannelState.NegotiatingTls negotiatingTls) {
            if (sslEvt.isSuccess()) {
                toForwarding(negotiatingTls.toForwarding());
            }
            else {
                toClosed(sslEvt.cause());
            }
        }
        else {
            illegalState("Server TLS handshake complete when not in the negotiating TLS state");
        }
    }

    void illegalState(@NonNull String msg) {
        if (!(state instanceof ProxyChannelState.Closed)) {
            IllegalStateException exception = new IllegalStateException("While in state " + state + ": " + msg);
            LOGGER.error("Illegal state, closing channels with no client response", exception);
            toClosed(null);
        }
    }

    void forwardToClient(Object msg) {
        Objects.requireNonNull(frontendHandler).forwardToClient(msg);
    }

    void forwardToServer(Object msg) {
        Objects.requireNonNull(backendHandler).forwardToServer(msg);
    }

    void onClientRequest(@NonNull SaslDecodePredicate dp,
                         Object msg) {
        Objects.requireNonNull(frontendHandler);
        if (state() instanceof ProxyChannelState.Forwarding) { // post-backend connection
            forwardToServer(msg);
        }
        else {
            frontendHandler.bufferMsg(msg);
            if (state() instanceof ProxyChannelState.ClientActive clientActive) {
                if (msg instanceof HAProxyMessage haProxyMessage) {
                    toHaProxy(clientActive.toHaProxy(haProxyMessage));
                    return;
                }
                else if (msg instanceof DecodedRequestFrame
                        && ((DecodedRequestFrame<?>) msg).apiKey() == ApiKeys.API_VERSIONS) {
                    DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame = (DecodedRequestFrame<ApiVersionsRequestData>) msg;
                    if (dp.isAuthenticationOffloadEnabled()) {
                        toApiVersions(clientActive.toApiVersions(apiVersionsFrame), apiVersionsFrame);
                    }
                    else {
                        toSelectingServer(clientActive.toSelectingServer(apiVersionsFrame));
                    }
                    return;
                }
                else if (msg instanceof RequestFrame) {
                    toSelectingServer(clientActive.toSelectingServer(null));
                    return;
                }
            }
            else if (state() instanceof ProxyChannelState.HaProxy haProxy) {
                if (msg instanceof DecodedRequestFrame
                        && ((DecodedRequestFrame<?>) msg).apiKey() == ApiKeys.API_VERSIONS) {
                    DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame = (DecodedRequestFrame<ApiVersionsRequestData>) msg;
                    if (dp.isAuthenticationOffloadEnabled()) {
                        toApiVersions(haProxy.toApiVersions(apiVersionsFrame), apiVersionsFrame);
                    }
                    else {
                        toSelectingServer(haProxy.toSelectingServer(apiVersionsFrame));
                    }
                    return;
                }
                else if (msg instanceof RequestFrame) {
                    toSelectingServer(haProxy.toSelectingServer(null));
                    return;
                }
            }
            else if (state() instanceof ProxyChannelState.ApiVersions apiVersions) {
                if (msg instanceof RequestFrame) {
                    if (dp.isAuthenticationOffloadEnabled()) {
                        // TODO if dp.isAuthenticationOffloadEnabled() then we need to forward to that handler
                        // TODO we only do the connection once we know the authenticated identity
                        toSelectingServer(apiVersions.toSelectingServer());
                    }
                    else {
                        toSelectingServer(apiVersions.toSelectingServer());
                    }
                    return;
                }
            }
            else if (state() instanceof ProxyChannelState.SelectingServer) {
                if (msg instanceof RequestFrame) {
                    return;
                }
            }
            else if (state() instanceof ProxyChannelState.Connecting) {
                if (msg instanceof RequestFrame) {
                    return;
                }
            }
            else if (state() instanceof ProxyChannelState.NegotiatingTls) {
                if (msg instanceof RequestFrame) {
                    return;
                }
            }
            illegalState("Unexpected message received: " + (msg == null ? "null" : "message class=" + msg.getClass()));
        }
    }

    private void toHaProxy(ProxyChannelState.HaProxy haProxy) {
        setState(haProxy);
    }

    private void toApiVersions(ProxyChannelState.ApiVersions apiVersions, DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame) {
        setState(apiVersions);
        frontendHandler.inApiVersions(apiVersionsFrame);
    }

    private void toSelectingServer(ProxyChannelState.SelectingServer selectingServer) {
        setState(selectingServer);
        frontendHandler.inSelectingServer();
    }

    boolean isConnecting() {
        return state instanceof ProxyChannelState.Connecting;
    }

    boolean isSelectingServer() {
        return state instanceof ProxyChannelState.SelectingServer;
    }

    void onServerInactive() {
        toClosed(null);
    }

    void serverReadComplete() {
        Objects.requireNonNull(frontendHandler).flushToClient();
    }

    void clientReadComplete() {
        if (backendHandler != null) {
            backendHandler.flushToServer();
        }
    }

    private void toClosed(@Nullable Throwable errorCodeEx) {
        // Close the server connection
        if (backendHandler != null) {
            backendHandler.close();
        }

        // Close the client connection with any error code
        if (frontendHandler != null) {
            frontendHandler.closeWithResponse(errorCodeEx);
        }

        setState(new ProxyChannelState.Closed());
    }

    void onServerException(Throwable cause) {
        LOGGER.atWarn()
                .setCause(LOGGER.isDebugEnabled() ? cause : null)
                .addArgument(cause != null ? cause.getMessage() : "")
                .log("Exception from the server channel: {}. Increase log level to DEBUG for stacktrace");
        toClosed(cause);
    }

    void onClientException(Throwable cause, boolean tlsEnabled) {
        ApiException errorCodeEx;
        if (cause instanceof DecoderException de
                && de.getCause() instanceof FrameOversizedException e) {
            var tlsHint = tlsEnabled ? "" : " or an unexpected TLS handshake";
            LOGGER.warn(
                    "Received over-sized frame from the client, max frame size bytes {}, received frame size bytes {} "
                            + "(hint: are we decoding a Kafka frame, or something unexpected like an HTTP request{}?)",
                    e.getMaxFrameSizeBytes(), e.getReceivedFrameSizeBytes(), tlsHint);
            errorCodeEx = Errors.INVALID_REQUEST.exception();
        }
        else {
            LOGGER.atWarn()
                    .setCause(LOGGER.isDebugEnabled() ? cause : null)
                    .addArgument(cause != null ? cause.getMessage() : "")
                    .log("Exception from the client channel: {}. Increase log level to DEBUG for stacktrace");
            errorCodeEx = Errors.UNKNOWN_SERVER_ERROR.exception();
        }
        toClosed(errorCodeEx);
    }

    void onClientInactive() {
        toClosed(null);
    }

    @Override
    public String toString() {
        return "StateHolder{" +
                "state=" + state +
                ", serverReadsBlocked=" + serverReadsBlocked +
                ", clientReadsBlocked=" + clientReadsBlocked +
                ", frontendHandler=" + frontendHandler +
                ", backendHandler=" + backendHandler +
                '}';
    }
}
