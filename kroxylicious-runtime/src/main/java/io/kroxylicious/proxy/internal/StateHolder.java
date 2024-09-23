/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import edu.umd.cs.findbugs.annotations.NonNull;

import edu.umd.cs.findbugs.annotations.Nullable;

import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.haproxy.HAProxyMessage;

import io.netty.handler.ssl.SslContext;

import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;

import java.util.List;

public class StateHolder {
    private ProxyChannelState state;
    private boolean clientBlocked;
    private boolean serverBlocked;
    private KafkaProxyFrontendHandler frontendHandler;
    private KafkaProxyBackendHandler backendHandler;

    void onClientBlocked() {
        clientBlocked = true;
        backendHandler.inBlocked();
    }

    void onClientUnblocked() {
        clientBlocked = false;
        backendHandler.inUnblocked();
    }

    void onServerBlocked() {
        serverBlocked = true;
        frontendHandler.inBlocked();
    }

    void onServerUnblocked() {
        serverBlocked = false;
        frontendHandler.inUnblocked();
    }

    /////////////////



    public StateHolder() {
    }


    @VisibleForTesting
    ProxyChannelState state() {
        return state;
    }

    @VisibleForTesting
    void setState(@NonNull ProxyChannelState state) {
        this.state = state;
    }

    @VisibleForTesting
    void onClientActive(@NonNull KafkaProxyFrontendHandler frontendHandler) {
        if (this.state == null) {
            this.frontendHandler = frontendHandler;
            this.clientBlocked = false;
            setState(new ProxyChannelState.ClientActive());
            frontendHandler.inClientActive();
        }
        else {
            illegalState("Client activation while not in the start state");
            throw new IllegalStateException();
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
        if (this.state instanceof ProxyChannelState.ClientActive ca) {
            setState(ca.toApiVersions(apiVersionsFrame));
        }
        else if (this.state instanceof ProxyChannelState.HaProxy ca) {
            setState(ca.toApiVersions(apiVersionsFrame));
        }
        else {
            illegalState("");
            throw new IllegalStateException();
        }
        frontendHandler.inApiVersions(apiVersionsFrame);
    }

    ProxyChannelState.Connecting onServerSelected(
            @NonNull HostPort remote,
            @NonNull List<FilterAndInvoker> filters,
            VirtualCluster virtualCluster) {
        if (state instanceof ProxyChannelState.SelectingServer selectingServerState) {
            ProxyChannelState.Connecting connecting = selectingServerState.toConnecting(remote);
            setState(connecting);
            return connecting;
        }
        else {
            // TODO why do we do the state change here, rather than
            // throwing ISE like the other methods overridden from
            // NFC, then just catch ISE in selectServer?
            String msg = "NetFilter called NetFilterContext.initiateConnect() more than once";
            illegalState(msg + " : filter='" + netFilter + "'");
            throw new IllegalStateException(msg);
        }

        backendHandler = new KafkaProxyBackendHandler(this, this.frontendHandler, virtualCluster);
        frontendHandler.inConnecting(remote, filters, backendHandler);
    }

    void onServerActive(ChannelHandlerContext serverCtx,
                        @Nullable SslContext sslContext) {
        if (state() instanceof ProxyChannelState.Connecting connectedState) {
            if (sslContext != null) {
                setState(connectedState.toNegotiatingTls(serverCtx));
            }
            else {
                setState(connectedState.toForwarding(serverCtx));
            }
        }
        else {
            illegalState("NetFilter didn't call NetFilterContext.initiateConnect(): filter='" + netFilter + "'");
        }
    }

    public void onServerTlsHandshakeCompletion(SslHandshakeCompletionEvent sslEvt) {
        if (state instanceof ProxyChannelState.NegotiatingTls negotiatingTls) {
            if (sslEvt.isSuccess()) {
                setState(negotiatingTls.toForwarding());
                frontendHandler.inForwarding();
            }
            else {
                // TODO error close
                closeServerAndClientChannels(null);
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
            closeServerAndClientChannels(null);
        }
    }

    void forwardToClient(Object msg) {
        frontendHandler.forwardToClient(msg);
    }

    void forwardToServer(ChannelHandlerContext clientCtx, Object msg) {
        backendHandler.forwardToServer(clientCtx, msg);
    }

    public void onRequest(SaslDecodePredicate dp,
                          ChannelHandlerContext ctx,
                          Object msg) {
        if (state() instanceof ProxyChannelState.Forwarding) { // post-backend connection
            frontendHandler.forwardToServer(ctx, msg);
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
        frontendHandler.upstreamChannelInactive(ctx);
        // TODO make this right
    }

    public void onServerException(ChannelHandlerContext ctx, Throwable cause) {
        frontendHandler.upstreamExceptionCaught(ctx, cause);
        // TODO make this right
    }

    public void serverReadComplete() {
        frontendHandler.flushToClient();
    }

    public void clientReadComplete() {
        backendHandler.flushToServer();
    }
}
