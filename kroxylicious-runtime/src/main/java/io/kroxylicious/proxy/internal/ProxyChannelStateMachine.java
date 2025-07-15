/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.haproxy.HAProxyMessage;

import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.ProxyChannelState.Closed;
import io.kroxylicious.proxy.internal.ProxyChannelState.Forwarding;
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.internal.util.StableKroxyliciousLinkGenerator;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.proxy.internal.ProxyChannelState.Startup.STARTING_STATE;
import static io.kroxylicious.proxy.internal.util.Metrics.KROXYLICIOUS_DOWNSTREAM_CONNECTIONS;
import static io.kroxylicious.proxy.internal.util.Metrics.KROXYLICIOUS_DOWNSTREAM_ERRORS;
import static io.kroxylicious.proxy.internal.util.Metrics.KROXYLICIOUS_UPSTREAM_CONNECTIONS;
import static io.kroxylicious.proxy.internal.util.Metrics.KROXYLICIOUS_UPSTREAM_CONNECTION_ATTEMPTS;
import static io.kroxylicious.proxy.internal.util.Metrics.KROXYLICIOUS_UPSTREAM_CONNECTION_FAILURES;
import static io.kroxylicious.proxy.internal.util.Metrics.KROXYLICIOUS_UPSTREAM_ERRORS;
import static io.kroxylicious.proxy.internal.util.Metrics.taggedCounter;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * <p>The state machine for a single client's proxy session.
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
 *      ↓ netFilter.{@link NetFilter#selectServer(NetFilter.NetFilterContext) selectServer} calls frontend.{@link KafkaProxyFrontendHandler#initiateConnect(HostPort, List) initiateConnect}
 *     {@link ProxyChannelState.Connecting Connecting} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *      │
 *      ↓
 *     {@link Forwarding Forwarding} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *      │ backend.{@link KafkaProxyBackendHandler#channelInactive(ChannelHandlerContext) channelInactive}
 *      │ or frontend.{@link KafkaProxyFrontendHandler#channelInactive(ChannelHandlerContext) channelInactive}
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
 * <p>Note that this backpressure state machine is not tied to the
 * session state machine: in general backpressure could happen in
 * several of the session states and is independent of them.</p>
 *
 * <p>
 *     When either side of the proxy stats applying back pressure the proxy should propagate that fact to teh other peer.
 *     Thus when the proxy is notified that a peer is applying back pressure it results in action on the channel with the opposite peer.
 * </p>
 */
@SuppressWarnings("java:S1133")
public class ProxyChannelStateMachine {
    private static final String DUPLICATE_INITIATE_CONNECT_ERROR = "NetFilter called NetFilterContext.initiateConnect() more than once";
    private static final Logger LOGGER = getLogger(ProxyChannelStateMachine.class);
    /**
     * @deprecated use `clientToProxyConnectionCounter` instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    private final Counter downstreamConnectionsCounter;
    /**
     * @deprecated use `proxyToServerConnectionCounter` instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    private final Counter upstreamConnectionsCounter;
    /**
     * @deprecated use `clientToProxyErrorCounter` instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    private final Counter downstreamErrorCounter;
    /**
     * @deprecated use `proxyToServerErrorCounter` instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    private final Counter upstreamErrorCounter;
    /**
     * @deprecated use `proxyToServerConnectionCounter` instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    private final Counter connectionAttemptsCounter;
    /**
     * @deprecated use `proxyToServerConnectionCounter` instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    private final Counter upstreamConnectionFailureCounter;

    // New connection metrics
    private final Counter clientToProxyErrorCounter;
    private final Counter clientToProxyConnectionCounter;
    private final Counter proxyToServerConnectionCounter;
    private final Counter proxyToServerErrorCounter;

    @SuppressWarnings("java:S5738")
    public ProxyChannelStateMachine(String clusterName, @Nullable Integer nodeId) {
        // New connection metrics
        clientToProxyConnectionCounter = Metrics.clientToProxyConnectionCounter(clusterName, nodeId).withTags();
        clientToProxyErrorCounter = Metrics.clientToProxyErrorCounter(clusterName, nodeId).withTags();
        proxyToServerConnectionCounter = Metrics.proxyToServerConnectionCounter(clusterName, nodeId).withTags();
        proxyToServerErrorCounter = Metrics.proxyToServerErrorCounter(clusterName, nodeId).withTags();

        // These connections metrics are deprecated and are replaced by the metrics mentioned above
        List<Tag> tags = Metrics.tags(Metrics.DEPRECATED_VIRTUAL_CLUSTER_TAG, clusterName);
        downstreamConnectionsCounter = taggedCounter(KROXYLICIOUS_DOWNSTREAM_CONNECTIONS, tags);
        downstreamErrorCounter = taggedCounter(KROXYLICIOUS_DOWNSTREAM_ERRORS, tags);
        upstreamConnectionsCounter = taggedCounter(KROXYLICIOUS_UPSTREAM_CONNECTIONS, tags);
        connectionAttemptsCounter = taggedCounter(KROXYLICIOUS_UPSTREAM_CONNECTION_ATTEMPTS, tags);
        upstreamErrorCounter = taggedCounter(KROXYLICIOUS_UPSTREAM_ERRORS, tags);
        upstreamConnectionFailureCounter = taggedCounter(KROXYLICIOUS_UPSTREAM_CONNECTION_FAILURES, tags);
    }

    /**
     * The current state. This can be changed via a call to one of the {@code on*()} methods.
     */
    @NonNull
    private ProxyChannelState state = STARTING_STATE;

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
    @SuppressWarnings({ "DataFlowIssue", "java:S2637" })
    @NonNull
    private KafkaProxyFrontendHandler frontendHandler = null;

    /**
     * The backend handler. Non-null if {@link #onNetFilterInitiateConnect(HostPort, List, VirtualClusterModel, NetFilter)}
     * has been called
     */
    @VisibleForTesting
    @Nullable
    private KafkaProxyBackendHandler backendHandler;

    ProxyChannelState state() {
        return state;
    }

    /**
     * Purely for tests DO NOT USE IN PRODUCTION code!!
     * Sonar will complain if one uses this in prod code listen to it.
     */
    @VisibleForTesting
    void forceState(@NonNull ProxyChannelState state, @NonNull KafkaProxyFrontendHandler frontendHandler, @Nullable KafkaProxyBackendHandler backendHandler) {
        LOGGER.info("Forcing state to {} with {} and {}", state, frontendHandler, backendHandler);
        this.state = state;
        this.frontendHandler = frontendHandler;
        this.backendHandler = backendHandler;
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

    public String currentState() {
        return this.state().getClass().getSimpleName();
    }

    /**
     * Notify the state machine when the client applies back pressure.
     */
    public void onClientUnwritable() {
        if (!serverReadsBlocked) {
            serverReadsBlocked = true;
            Objects.requireNonNull(backendHandler).applyBackpressure();
        }
    }

    /**
     * Notify the state machine when the client stops applying back pressure
     */
    public void onClientWritable() {
        if (serverReadsBlocked) {
            serverReadsBlocked = false;
            Objects.requireNonNull(backendHandler).relieveBackpressure();
        }
    }

    /**
     * Notify the state machine when the server applies back pressure
     */
    public void onServerUnwritable() {
        if (!clientReadsBlocked) {
            clientReadsBlocked = true;
            frontendHandler.applyBackpressure();
        }
    }

    /**
     * Notify the state machine when the server stops applying back pressure
     */
    public void onServerWritable() {
        if (clientReadsBlocked) {
            clientReadsBlocked = false;
            frontendHandler.relieveBackpressure();
        }
    }

    /**
     * Notify the statemachine that the client channel has an active TCP connection.
     * @param frontendHandler with active connection
     */
    void onClientActive(@NonNull KafkaProxyFrontendHandler frontendHandler) {
        if (STARTING_STATE.equals(this.state)) {
            this.frontendHandler = frontendHandler;
            toClientActive(STARTING_STATE.toClientActive(), frontendHandler);
        }
        else {
            illegalState("Client activation while not in the start state");
        }
    }

    /**
     * Notify the statemachine that the netfilter has chosen an outbound peer.
     * @param peer the upstream host to connect to.
     * @param filters the set of filters to be applied to the session
     * @param virtualClusterModel the virtual cluster the client is connecting too
     * @param netFilter the netFilter which selected the upstream peer.
     */
    void onNetFilterInitiateConnect(
                                    @NonNull HostPort peer,
                                    @NonNull List<FilterAndInvoker> filters,
                                    VirtualClusterModel virtualClusterModel,
                                    NetFilter netFilter) {
        if (state instanceof ProxyChannelState.SelectingServer selectingServerState) {
            toConnecting(selectingServerState.toConnecting(peer), filters, virtualClusterModel);
        }
        else {
            illegalState(DUPLICATE_INITIATE_CONNECT_ERROR + " : netFilter='" + netFilter + "'");
        }
    }

    /**
     * Notify the statemachine that the upstream connection is ready for RPC calls.
     */
    void onServerActive() {
        if (state() instanceof ProxyChannelState.Connecting connectedState) {
            toForwarding(connectedState.toForwarding());
        }
        else {
            illegalState("Server became active while not in the connecting state");
        }
    }

    /**
     * <p>Notify the state machine of an unexpected event.
     * The definition of unexpected events is up to the callers.
     * An example would be trying to forward an event upstream before the upstream connection is established.
     * </p>
     * <p>illegalState implies termination of the proxy session. As this really represents a programming error NO error messages are propagated to clients.</p>
     *
     * @param msg the message to be <em>logged</em> in explanation of the error condition
     */
    void illegalState(@NonNull String msg) {
        if (!(state instanceof Closed)) {
            LOGGER.error("Unexpected event while in {} message: {}, closing channels with no client response.", state, msg);
            toClosed(null);
        }
    }

    /**
     * A message has been received from the upstream node which should be passed to the downstream client
     * @param msg the object received from the upstream
     */
    void messageFromServer(Object msg) {
        Objects.requireNonNull(frontendHandler).forwardToClient(msg);
    }

    /**
     * Called to notify the state machine that reading the upstream batch is complete.
     */
    void serverReadComplete() {
        Objects.requireNonNull(frontendHandler).flushToClient();
    }

    /**
     * A message has been received from the downstream client which should be passed to the upstream node
     * @param msg the RPC received from the upstream
     */
    void messageFromClient(Object msg) {
        Objects.requireNonNull(backendHandler).forwardToServer(msg);
    }

    /**
     * Called to notify the state machine that reading the downstream the batch is complete.
     */
    void clientReadComplete() {
        if (state instanceof Forwarding) {
            Objects.requireNonNull(backendHandler).flushToServer();
        }
    }

    /**
     * The proxy has received something from the client. The current state of the session determines what happens to it.
     * @param dp the decode predicate to be used if the session is still being negotiated
     * @param msg the RPC received from the downstream client
     */
    void onClientRequest(
                         @NonNull SaslDecodePredicate dp,
                         Object msg) {
        Objects.requireNonNull(frontendHandler);
        if (state() instanceof Forwarding) { // post-backend connection
            messageFromClient(msg);
        }
        else if (!onClientRequestBeforeForwarding(dp, msg)) {
            illegalState("Unexpected message received: " + (msg == null ? "null" : "message class=" + msg.getClass()));
        }
    }

    /**
     * ensure the state machine is in the connecting state.
     * @param msg to be logged if in another state.
     */
    void assertIsConnecting(String msg) {
        if (!(state instanceof ProxyChannelState.Connecting)) {
            illegalState(msg);
        }
    }

    /**
     * ensure the state machine is in the selecting server state.
     *
     * @return the SelectingServer state
     * @throws IllegalStateException if the state is not {@link ProxyChannelState.SelectingServer}.
     */
    ProxyChannelState.SelectingServer enforceInSelectingServer(String errorMessage) {
        if (state instanceof ProxyChannelState.SelectingServer selectingServerState) {
            return selectingServerState;
        }
        else {
            illegalState(errorMessage);
            throw new IllegalStateException("State required to be "
                    + ProxyChannelState.SelectingServer.class.getSimpleName()
                    + " but was "
                    + currentState()
                    + ":"
                    + errorMessage);
        }
    }

    /**
     * Notify the statemachine that the connection to the upstream node has been disconnected.
     * <p>
     * This will result in the proxy session being torn down.
     * </p>
     */
    void onServerInactive() {
        toClosed(null);
    }

    /**
     * Notify the statemachine that the connection to the downstream client has been disconnected.
     * <p>
     * This will result in the proxy session being torn down.
     * </p>
     */
    void onClientInactive() {
        toClosed(null);
    }

    /**
     * Notify the state machine that something exceptional and un-recoverable has happened on the upstream side.
     * @param cause the exception that triggered the issue
     */
    @SuppressWarnings("java:S5738")
    void onServerException(Throwable cause) {
        LOGGER.atWarn()
                .setCause(LOGGER.isDebugEnabled() ? cause : null)
                .addArgument(cause != null ? cause.getMessage() : "")
                .log("Exception from the server channel: {}. Increase log level to DEBUG for stacktrace");
        if (state instanceof ProxyChannelState.Connecting) {
            upstreamConnectionFailureCounter.increment();
        }
        upstreamErrorCounter.increment();
        proxyToServerErrorCounter.increment();
        toClosed(cause);
    }

    /**
     * Notify the state machine that something exceptional and un-recoverable has happened on the downstream side.
     * @param cause the exception that triggered the issue
     */
    @SuppressWarnings("java:S5738")
    void onClientException(Throwable cause, boolean tlsEnabled) {
        ApiException errorCodeEx;
        if (cause instanceof DecoderException de
                && de.getCause() instanceof FrameOversizedException e) {
            String tlsHint;
            tlsHint = tlsEnabled
                    ? ""
                    : " Possible unexpected TLS handshake? When connecting via TLS from your client, make sure to enable TLS for the Kroxylicious gateway ("
                            + StableKroxyliciousLinkGenerator.errorLink(StableKroxyliciousLinkGenerator.CLIENT_TLS)
                            + ").";
            LOGGER.warn(
                    "Received over-sized frame from the client, max frame size bytes {}, received frame size bytes {} "
                            + "(hint: {} Other possible causes are: an oversized Kafka frame, or something unexpected like an HTTP request.)",
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
        downstreamErrorCounter.increment();
        clientToProxyErrorCounter.increment();
        toClosed(errorCodeEx);
    }

    @SuppressWarnings("java:S5738")
    private void toClientActive(
                                @NonNull ProxyChannelState.ClientActive clientActive,
                                @NonNull KafkaProxyFrontendHandler frontendHandler) {
        setState(clientActive);
        frontendHandler.inClientActive();
        downstreamConnectionsCounter.increment();
        clientToProxyConnectionCounter.increment();
    }

    @SuppressWarnings("java:S5738")
    private void toConnecting(
                              ProxyChannelState.Connecting connecting,
                              @NonNull List<FilterAndInvoker> filters,
                              VirtualClusterModel virtualClusterModel) {
        setState(connecting);
        backendHandler = new KafkaProxyBackendHandler(this, virtualClusterModel);
        frontendHandler.inConnecting(connecting.remote(), filters, backendHandler);
        connectionAttemptsCounter.increment();
        proxyToServerConnectionCounter.increment();
    }

    @SuppressWarnings("java:S5738")
    private void toForwarding(Forwarding forwarding) {
        setState(forwarding);
        Objects.requireNonNull(frontendHandler).inForwarding();
        upstreamConnectionsCounter.increment();
    }

    /**
     * handle a message received from the client prior to connecting to the upstream node
     * @param dp DecodePredicate to cope with SASL offload
     * @param msg Message received from the downstream client.
     * @return <code>false</code> for unsupported message types
     */
    private boolean onClientRequestBeforeForwarding(@NonNull SaslDecodePredicate dp, Object msg) {
        frontendHandler.bufferMsg(msg);
        if (state() instanceof ProxyChannelState.ClientActive clientActive) {
            return onClientRequestInClientActiveState(dp, msg, clientActive);
        }
        else if (state() instanceof ProxyChannelState.HaProxy haProxy) {
            return onClientRequestInHaProxyState(dp, msg, haProxy);
        }
        else if (state() instanceof ProxyChannelState.ApiVersions apiVersions) {
            return onClientRequestInApiVersionsState(dp, msg, apiVersions);
        }
        else if (state() instanceof ProxyChannelState.SelectingServer) {
            return msg instanceof RequestFrame;
        }
        else {
            return state() instanceof ProxyChannelState.Connecting && msg instanceof RequestFrame;
        }
    }

    @SuppressWarnings({ "java:S1172", "java:S1135" })
    // We keep dp as we should need it and it gives consistency with the other onClientRequestIn methods (sue me)
    private boolean onClientRequestInApiVersionsState(@NonNull SaslDecodePredicate dp, Object msg, ProxyChannelState.ApiVersions apiVersions) {
        if (msg instanceof RequestFrame) {
            // TODO if dp.isAuthenticationOffloadEnabled() then we need to forward to that handler
            // TODO we only do the connection once we know the authenticated identity
            toSelectingServer(apiVersions.toSelectingServer());
            return true;
        }
        return false;
    }

    private boolean onClientRequestInHaProxyState(@NonNull SaslDecodePredicate dp, Object msg, ProxyChannelState.HaProxy haProxy) {
        return transitionClientRequest(dp, msg, haProxy::toApiVersions, haProxy::toSelectingServer);
    }

    private boolean transitionClientRequest(
                                            @NonNull SaslDecodePredicate dp,
                                            Object msg,
                                            Function<DecodedRequestFrame<ApiVersionsRequestData>, ProxyChannelState.ApiVersions> apiVersionsFactory,
                                            Function<DecodedRequestFrame<ApiVersionsRequestData>, ProxyChannelState.SelectingServer> selectingServerFactory) {
        if (isMessageApiVersionsRequest(msg)) {
            // We know it's an API Versions request even if the compiler doesn't
            @SuppressWarnings("unchecked")
            DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame = (DecodedRequestFrame<ApiVersionsRequestData>) msg;
            if (dp.isAuthenticationOffloadEnabled()) {
                toApiVersions(apiVersionsFactory.apply(apiVersionsFrame), apiVersionsFrame);
            }
            else {
                toSelectingServer(selectingServerFactory.apply(apiVersionsFrame));
            }
            return true;
        }
        else if (msg instanceof RequestFrame) {
            toSelectingServer(selectingServerFactory.apply(null));
            return true;
        }
        return false;
    }

    private boolean onClientRequestInClientActiveState(@NonNull SaslDecodePredicate dp, Object msg, ProxyChannelState.ClientActive clientActive) {
        if (msg instanceof HAProxyMessage haProxyMessage) {
            toHaProxy(clientActive.toHaProxy(haProxyMessage));
            return true;
        }
        else {
            return transitionClientRequest(dp, msg, clientActive::toApiVersions, clientActive::toSelectingServer);
        }
    }

    private void toHaProxy(ProxyChannelState.HaProxy haProxy) {
        setState(haProxy);
    }

    private void toApiVersions(
                               ProxyChannelState.ApiVersions apiVersions,
                               DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame) {
        setState(apiVersions);
        Objects.requireNonNull(frontendHandler).inApiVersions(apiVersionsFrame);
    }

    private void toSelectingServer(ProxyChannelState.SelectingServer selectingServer) {
        setState(selectingServer);
        Objects.requireNonNull(frontendHandler).inSelectingServer();
    }

    @SuppressWarnings("ConstantValue")
    private void toClosed(@Nullable Throwable errorCodeEx) {
        if (state instanceof Closed) {
            return;
        }
        setState(new Closed());
        // Close the server connection
        if (backendHandler != null) {
            backendHandler.inClosed();
        }

        // Close the client connection with any error code
        if (frontendHandler != null) { // Can be null if the error happens before clientActive (unlikely but possible)
            frontendHandler.inClosed(errorCodeEx);
        }
    }

    private void setState(@NonNull ProxyChannelState state) {
        LOGGER.trace("{} transitioning to {}", this, state);
        this.state = state;
    }

    private static boolean isMessageApiVersionsRequest(Object msg) {
        return msg instanceof DecodedRequestFrame
                && ((DecodedRequestFrame<?>) msg).apiKey() == ApiKeys.API_VERSIONS;
    }
}
