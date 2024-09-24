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

public class StateHolder {
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
     * The backend handler. Non-null if {@link #onServerSelected(HostPort, List, VirtualCluster, NetFilter)}
     * has been called
     */
    @Nullable KafkaProxyBackendHandler backendHandler;

    private final Logger LOGGER = getLogger(StateHolder.class);

    void onClientUnwritable() {
        if (!serverReadsBlocked) {
            serverReadsBlocked = true;
            backendHandler.blockServerReads();
        }
    }

    void onClientWritable() {
        if (serverReadsBlocked) {
            serverReadsBlocked = false;
            backendHandler.unblockServerReads();
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

    /////////////////

    public StateHolder() {
    }

    @VisibleForTesting
    ProxyChannelState state() {
        return state;
    }

    void setState(@NonNull ProxyChannelState state) {
        this.state = state;
    }

    @VisibleForTesting
    void onClientActive(@NonNull KafkaProxyFrontendHandler frontendHandler) {
        if (this.state == null) {
            this.frontendHandler = frontendHandler;
            setState(new ProxyChannelState.ClientActive());
            frontendHandler.inClientActive();
        }
        else {
            illegalState("Client activation while not in the start state");
        }

    }

    @VisibleForTesting
    void onHaProxy(@NonNull HAProxyMessage haProxyMessage) {
        if (this.state instanceof ProxyChannelState.ClientActive cs) {
            setState(cs.toHaProxy(haProxyMessage));
        }
        else {
            illegalState("");
        }
    }

    @VisibleForTesting
    void onApiVersionsReceived(@NonNull DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame) {
        ProxyChannelState.ApiVersions apiVersions;
        if (this.state instanceof ProxyChannelState.ClientActive ca) {
            apiVersions = ca.toApiVersions(apiVersionsFrame);
        }
        else if (this.state instanceof ProxyChannelState.HaProxy hap) {
            apiVersions = hap.toApiVersions(apiVersionsFrame);
        }
        else {
            illegalState("");
            return;
        }
        setState(apiVersions);
        Objects.requireNonNull(frontendHandler).inApiVersions(apiVersionsFrame);
    }

    void onServerSelected(
            @NonNull HostPort remote,
            @NonNull List<FilterAndInvoker> filters,
            VirtualCluster virtualCluster, NetFilter netFilter) {
        if (state instanceof ProxyChannelState.SelectingServer selectingServerState) {
            ProxyChannelState.Connecting connecting = selectingServerState.toConnecting(remote);
            setState(connecting);
            backendHandler = new KafkaProxyBackendHandler(this, virtualCluster);
            frontendHandler.inConnecting(remote, filters, backendHandler);
        }
        else {
            // TODO why do we do the state change here, rather than
            // throwing ISE like the other methods overridden from
            // NFC, then just catch ISE in selectServer?
            String msg = "NetFilter called NetFilterContext.initiateConnect() more than once";
            illegalState(msg + " : filter='" + netFilter + "'");
            throw new IllegalStateException(msg);
        }
    }

    void onServerActive(ChannelHandlerContext serverCtx,
                        @Nullable SslContext sslContext) {
        if (state() instanceof ProxyChannelState.Connecting connectedState) {
            if (sslContext != null) {
                setState(connectedState.toNegotiatingTls(serverCtx));
            }
            else {
                setState(connectedState.toForwarding(serverCtx));
                frontendHandler.inForwarding();
            }
        }
        else {
            illegalState("");
        }
    }

    public void onServerTlsHandshakeCompletion(SslHandshakeCompletionEvent sslEvt) {
        if (state instanceof ProxyChannelState.NegotiatingTls negotiatingTls) {
            if (sslEvt.isSuccess()) {
                setState(negotiatingTls.toForwarding());
                frontendHandler.inForwarding();
            }
            else {
                closeClientAndServerChannels(sslEvt.cause());
            }
        }
        else {
            illegalState("");
        }
    }

    void illegalState(@NonNull String msg) {
        if (!(state instanceof ProxyChannelState.Closed)) {
            IllegalStateException exception = new IllegalStateException("While in state " + state + ": " + msg);
            LOGGER.error("Illegal state, closing channels with no client response", exception);
            closeClientAndServerChannels(null);
        }
    }

    void forwardToClient(Object msg) {
        frontendHandler.forwardToClient(msg);
    }

    void forwardToServer(Object msg) {
        backendHandler.forwardToServer(msg);
    }

    public void onRequest(SaslDecodePredicate dp,
                          Object msg) {
        if (state() instanceof ProxyChannelState.Forwarding) { // post-backend connection
            frontendHandler.forwardToServer(msg);
        }
        else {
            frontendHandler.bufferMsg(msg);
            if (state() instanceof ProxyChannelState.ClientActive clientActive) {
                if (msg instanceof HAProxyMessage haProxyMessage) {
                    onHaProxy(haProxyMessage);
                    return;
                }
                else if (msg instanceof DecodedRequestFrame
                        && ((DecodedRequestFrame<?>) msg).apiKey() == ApiKeys.API_VERSIONS) {
                    DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame = (DecodedRequestFrame<ApiVersionsRequestData>) msg;
                    if (dp.isAuthenticationOffloadEnabled()) {
                        onApiVersionsReceived(apiVersionsFrame);
                    }
                    else {
                        setState(clientActive.toSelectingServer(apiVersionsFrame));
                        frontendHandler.inSelectingServer();
                    }
                    return;
                }
                else if (msg instanceof RequestFrame) {
                    setState(clientActive.toSelectingServer(null));
                    frontendHandler.inSelectingServer();
                    return;
                }
            }
            else if (state() instanceof ProxyChannelState.HaProxy haProxy) {
                if (msg instanceof DecodedRequestFrame
                        && ((DecodedRequestFrame<?>) msg).apiKey() == ApiKeys.API_VERSIONS) {
                    DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame = (DecodedRequestFrame<ApiVersionsRequestData>) msg;
                    if (dp.isAuthenticationOffloadEnabled()) {
                        onApiVersionsReceived(apiVersionsFrame);
                    }
                    else {
                        setState(haProxy.toSelectingServer(apiVersionsFrame));
                        frontendHandler.inSelectingServer();
                    }
                    return;
                }
                else if (msg instanceof RequestFrame) {
                    setState(haProxy.toSelectingServer(null));
                    frontendHandler.inSelectingServer();
                    return;
                }
            }
            else if (state() instanceof ProxyChannelState.ApiVersions apiVersions) {
                if (msg instanceof RequestFrame) {
                    if (dp.isAuthenticationOffloadEnabled()) {
                        // TODO if dp.isAuthenticationOffloadEnabled() then we need to forward to that handler
                        // TODO we only do the connection once we know the authenticated identity
                        setState(apiVersions.toSelectingServer());
                        frontendHandler.inSelectingServer();
                    }
                    else {
                        setState(apiVersions.toSelectingServer());
                        frontendHandler.inSelectingServer();
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

    public boolean isConnecting() {
        return state instanceof ProxyChannelState.Connecting;
    }

    public boolean isSelectingServer() {
        return state instanceof ProxyChannelState.SelectingServer;
    }

    public void onServerInactive(ChannelHandlerContext ctx) {
        frontendHandler.closeServerAndClientChannels(null);
        // TODO make this right
    }

    public void serverReadComplete() {
        frontendHandler.flushToClient();
    }

    public void clientReadComplete() {
        if (backendHandler != null) {
            backendHandler.flushToServer();
        }
    }

    private void closeClientAndServerChannels(@Nullable Throwable errorCodeEx) {
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

    public void onServerException(Throwable cause) {
        LOGGER.atWarn()
                .setCause(LOGGER.isDebugEnabled() ? cause : null)
                .addArgument(cause != null ? cause.getMessage() : "")
                .log("Exception from the server channel: {}. Increase log level to DEBUG for stacktrace");
        closeClientAndServerChannels(cause);
    }

    public void onClientException(Throwable cause, boolean tlsEnabled) {
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
        closeClientAndServerChannels(errorCodeEx);
    }
}
