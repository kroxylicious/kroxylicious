/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.List;
import java.util.Objects;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.haproxy.HAProxyMessage;

import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.ProxyChannelState.Closed;
import io.kroxylicious.proxy.internal.ProxyChannelState.Closing;
import io.kroxylicious.proxy.internal.ProxyChannelState.Forwarding;
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

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
 *      │
 *      ↓
 *     {@link Forwarding Forwarding} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *      │ backend.{@link KafkaProxyBackendHandler#channelInactive(ChannelHandlerContext) channelInactive}
 *      │ or frontend.{@link KafkaProxyFrontendHandler#channelInactive(ChannelHandlerContext) channelInactive}
 *      ↓
 *     {@link Closing Closing} ⇠╌╌╌╌ <b>error</b> ⇠╌╌╌╌⤍
 *      │
 *      ↓
 *     {@link Closed Closed} ⇠╌╌╌╌ <b>error</b> ⇠╌╌╌╌
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
public class ProxyChannelStateMachine {
    private static final Logger LOGGER = getLogger(ProxyChannelStateMachine.class);

    /**
     * The current state. This can be changed via a call to one of the {@code on*()} methods.
     */
    @VisibleForTesting
    @Nullable
    ProxyChannelState state;

    /*
     * The netty autoread flag is volatile =>
     * expensive to set in every call to channelRead.
     * So we track autoread states via these non-volatile fields,
     * allowing us to only touch the volatile when it needs to be changed
     */
    @VisibleForTesting
    boolean serverReadsBlocked;
    @VisibleForTesting
    boolean clientReadsBlocked;

    /**
     * The frontend handler. Non-null if we got as far as ClientActive.
     */
    @VisibleForTesting
    @NonNull
    KafkaProxyFrontendHandler frontendHandler;
    /**
     * The backend handler. Non-null if {@link #onNetFilterInitiateConnect(HostPort, List, VirtualCluster, NetFilter)}
     * has been called
     */
    @VisibleForTesting
    @Nullable
    KafkaProxyBackendHandler backendHandler;

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
            frontendHandler.blockClientReads();
        }
    }

    /**
     * The channel to the server is now writable
     */
    void onServerWritable() {
        if (clientReadsBlocked) {
            clientReadsBlocked = false;
            frontendHandler.unblockClientReads();
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
        frontendHandler.inConnecting(remote, filters, backendHandler);
    }

    void onServerActive() {
        if (state() instanceof ProxyChannelState.Connecting connectedState) {
            toForwarding(connectedState.toForwarding());
        }
        else {
            illegalState("Server became active while not in the connecting state");
        }
    }

    void onServerClosed() {
        LOGGER.info("server closed");
        if (state() instanceof ProxyChannelState.Closing closing) {
            setState(new Closing(closing.ErrorCode(), closing.clientClosed(), true).transition());
        }
        else {
            illegalState("Server became closed while not in the closing state");
        }
    }

    void onClientClosed() {
        LOGGER.info("Client Closed");
        if (state() instanceof ProxyChannelState.Closing closing) {
            setState(new Closing(closing.ErrorCode(), true, closing.serverClosed()).transition());
        }
        else {
            illegalState("Client became closed while not in the closing state");
        }
    }

    private void toForwarding(Forwarding forwarding) {
        setState(forwarding);
        Objects.requireNonNull(frontendHandler).inForwarding();
    }

    void illegalState(@NonNull String msg) {
        if (!(state instanceof Closed)) {
            IllegalStateException exception = new IllegalStateException("While in state " + state + ": " + msg);
            LOGGER.error("Illegal state, closing channels with no client response", exception);
            toClosing(null);
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
        if (state() instanceof Forwarding) { // post-backend connection
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
                    // TODO this isn't really a stateHolder responsibility
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
            illegalState("Unexpected message received: " + (msg == null ? "null" : "message class=" + msg.getClass()));
        }
    }

    private void toHaProxy(ProxyChannelState.HaProxy haProxy) {
        setState(haProxy);
    }

    private void toApiVersions(ProxyChannelState.ApiVersions apiVersions,
                               DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame) {
        setState(apiVersions);
        Objects.requireNonNull(frontendHandler).inApiVersions(apiVersionsFrame);
    }

    private void toSelectingServer(ProxyChannelState.SelectingServer selectingServer) {
        setState(selectingServer);
        Objects.requireNonNull(frontendHandler).inSelectingServer();
    }

    void assertIsConnecting(String msg) {
        if (!(state instanceof ProxyChannelState.Connecting)) {
            illegalState(msg);
        }
    }

    void assertIsSelectingServer(String msg) {
        if (!(state instanceof ProxyChannelState.SelectingServer)) {
            illegalState(msg);
        }
    }

    void onServerInactive() {
        toClosing(null);
    }

    void serverReadComplete() {
        Objects.requireNonNull(frontendHandler).flushToClient();
    }

    void clientReadComplete() {
        if (state instanceof Forwarding) {
            backendHandler.flushToServer();
        }
    }

    private void toClosing(@Nullable Throwable errorCodeEx) {
        if (state instanceof Closing || state instanceof Closed) {
            return;
        }
        setState(new Closing(errorCodeEx, false, false));
        // Close the server connection
        if (backendHandler != null) {
            backendHandler.inClosing();
        }

        // Close the client connection with any error code
        if (frontendHandler != null) {
            frontendHandler.inClosing(errorCodeEx);
        }
    }

    void onServerException(Throwable cause) {
        LOGGER.atWarn()
                .setCause(LOGGER.isDebugEnabled() ? cause : null)
                .addArgument(cause != null ? cause.getMessage() : "")
                .log("Exception from the server channel: {}. Increase log level to DEBUG for stacktrace");
        toClosing(cause);
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
        toClosing(errorCodeEx);
    }

    void onClientInactive() {
        toClosing(null);
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