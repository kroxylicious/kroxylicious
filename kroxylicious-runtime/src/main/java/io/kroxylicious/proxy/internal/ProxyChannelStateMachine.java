/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import javax.net.ssl.SSLSession;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.haproxy.HAProxyMessage;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.ProxyChannelState.Closed;
import io.kroxylicious.proxy.internal.ProxyChannelState.Forwarding;
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.util.ActivationToken;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.internal.util.StableKroxyliciousLinkGenerator;
import io.kroxylicious.proxy.internal.util.VirtualClusterNode;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.proxy.internal.ProxyChannelState.Startup.STARTING_STATE;
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
 *      ↓ frontend.{@link KafkaProxyFrontendHandler#channelRead(ChannelHandlerContext, Object) channelRead} receives any other KRPC request
 *     {@link ProxyChannelState.SelectingServer SelectingServer} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
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
    private static final String DUPLICATE_INITIATE_CONNECT_ERROR = "onInitiateConnect called more than once";
    private static final Logger LOGGER = getLogger(ProxyChannelStateMachine.class);

    /**
     * Enumeration of disconnect causes for tracking client to proxy disconnections.
     */
    public enum DisconnectCause {
        /** Connection closed due to exceeding idle timeout */
        IDLE_TIMEOUT("idle_timeout"),
        /** Client initiated connection close */
        CLIENT_CLOSED("client_closed"),
        /** Server (backend Kafka broker) initiated connection close */
        SERVER_CLOSED("server_closed");

        private final String label;

        DisconnectCause(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    // Connection metrics
    private final Counter clientToProxyErrorCounter;
    private final Counter clientToProxyDisconnectsIdleCounter;
    private final Counter clientToProxyDisconnectsClientClosedCounter;
    private final Counter clientToProxyDisconnectsServerClosedCounter;
    private final Counter clientToProxyConnectionCounter;
    private final Counter proxyToServerConnectionCounter;
    private final Counter proxyToServerErrorCounter;
    private final Timer serverToProxyBackpressureMeter;
    private final Timer clientToProxyBackPressureMeter;

    private final ActivationToken clientToProxyConnectionToken;
    private final ActivationToken proxyToServerConnectionToken;

    @VisibleForTesting
    @Nullable
    Timer.Sample clientToProxyBackpressureTimer;

    @VisibleForTesting
    @Nullable
    Timer.Sample serverBackpressureTimer;

    private final EndpointBinding endpointBinding;

    @NonNull
    // Ideally this would be final, however that breaks forceState which is used heavily in testing.
    private KafkaSession kafkaSession;

    /**
     * The current state. This can be changed via a call to one of the {@code on*()} methods.
     */
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
    private final TransportSubjectBuilder transportSubjectBuilder;
    private final ClientSubjectManager clientSubjectManager = new ClientSubjectManager();
    private int progressionLatch = -1;
    /**
     * The frontend handler. Non-null if we got as far as ClientActive.
     */
    @SuppressWarnings({ "java:S2637" })
    private @Nullable KafkaProxyFrontendHandler frontendHandler = null;

    /**
     * The backend handler. Non-null if {@link #onInitiateConnect(HostPort)}
     * has been called
     */
    @VisibleForTesting
    @Nullable
    private KafkaProxyBackendHandler backendHandler;

    public ProxyChannelStateMachine(EndpointBinding endpointBinding,
                                    TransportSubjectBuilder transportSubjectBuilder) {
        this.endpointBinding = endpointBinding;
        this.transportSubjectBuilder = transportSubjectBuilder;
        var virtualCluster = endpointBinding.endpointGateway().virtualCluster();
        kafkaSession = new KafkaSession(KafkaSessionState.ESTABLISHING);

        var nodeId = endpointBinding.nodeId();
        String clusterName = virtualCluster.getClusterName();
        VirtualClusterNode node = new VirtualClusterNode(clusterName, nodeId);
        // Connection metrics
        clientToProxyConnectionCounter = Metrics.clientToProxyConnectionCounter(clusterName, nodeId).withTags();
        clientToProxyDisconnectsIdleCounter = Metrics.clientToProxyDisconnectsCounter(clusterName, nodeId, DisconnectCause.IDLE_TIMEOUT.label()).withTags();
        clientToProxyDisconnectsClientClosedCounter = Metrics.clientToProxyDisconnectsCounter(clusterName, nodeId, DisconnectCause.CLIENT_CLOSED.label()).withTags();
        clientToProxyDisconnectsServerClosedCounter = Metrics.clientToProxyDisconnectsCounter(clusterName, nodeId, DisconnectCause.SERVER_CLOSED.label()).withTags();
        clientToProxyErrorCounter = Metrics.clientToProxyErrorCounter(clusterName, nodeId).withTags();
        proxyToServerConnectionCounter = Metrics.proxyToServerConnectionCounter(clusterName, nodeId).withTags();
        proxyToServerErrorCounter = Metrics.proxyToServerErrorCounter(clusterName, nodeId).withTags();
        serverToProxyBackpressureMeter = Metrics.serverToProxyBackpressureTimer(clusterName, nodeId).withTags();
        clientToProxyBackPressureMeter = Metrics.clientToProxyBackpressureTimer(clusterName, nodeId).withTags();
        clientToProxyConnectionToken = Metrics.clientToProxyConnectionToken(node);
        proxyToServerConnectionToken = Metrics.proxyToServerConnectionToken(node);
    }

    ProxyChannelState state() {
        return state;
    }

    /**
     * Purely for tests DO NOT USE IN PRODUCTION code!!
     * Sonar will complain if one uses this in prod code listen to it.
     */
    @VisibleForTesting
    void forceState(ProxyChannelState state, KafkaProxyFrontendHandler frontendHandler, @Nullable KafkaProxyBackendHandler backendHandler, KafkaSession kafkaSession) {
        LOGGER.info("Forcing state to {} with {} and {}", state, frontendHandler, backendHandler);
        this.state = state;
        this.kafkaSession = kafkaSession;
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

    @Nullable
    Integer nodeId() {
        return endpointBinding.nodeId();
    }

    String clusterName() {
        return virtualCluster().getClusterName();
    }

    EndpointBinding endpointBinding() {
        return endpointBinding;
    }

    EndpointGateway endpointGateway() {
        return endpointBinding.endpointGateway();
    }

    VirtualClusterModel virtualCluster() {
        return endpointBinding.endpointGateway().virtualCluster();
    }

    boolean isTlsListener() {
        return endpointBinding.endpointGateway().isUseTls();
    }

    /**
     * Notify the state machine when the client applies back pressure.
     */
    public void onClientUnwritable() {
        if (!serverReadsBlocked) {
            serverReadsBlocked = true;
            serverBackpressureTimer = Timer.start();
            Objects.requireNonNull(backendHandler).applyBackpressure();
        }
    }

    /**
     * Notify the state machine when the client stops applying back pressure
     */
    public void onClientWritable() {
        if (serverReadsBlocked) {
            serverReadsBlocked = false;
            if (serverBackpressureTimer != null) {
                serverBackpressureTimer.stop(serverToProxyBackpressureMeter);
                serverBackpressureTimer = null;
            }
            Objects.requireNonNull(backendHandler).relieveBackpressure();
        }
    }

    /**
     * Notify the state machine when the server applies back pressure
     */
    public void onServerUnwritable() {
        if (!clientReadsBlocked) {
            clientReadsBlocked = true;
            clientToProxyBackpressureTimer = Timer.start();
            Objects.requireNonNull(frontendHandler).applyBackpressure();
        }
    }

    /**
     * Notify the state machine when the server stops applying back pressure
     */
    public void onServerWritable() {
        if (clientReadsBlocked) {
            clientReadsBlocked = false;
            if (clientToProxyBackpressureTimer != null) {
                clientToProxyBackpressureTimer.stop(clientToProxyBackPressureMeter);
                clientToProxyBackpressureTimer = null;
            }
            Objects.requireNonNull(frontendHandler).relieveBackpressure();
        }
    }

    /**
     * Notify the statemachine that the client channel has an active TCP connection.
     * @param frontendHandler with active connection
     */
    void onClientActive(KafkaProxyFrontendHandler frontendHandler) {
        if (STARTING_STATE.equals(this.state)) {
            this.frontendHandler = frontendHandler;
            LOGGER.atDebug()
                    .setMessage("Allocated session ID: {} for downstream connection from {}:{}")
                    .addArgument(kafkaSession.sessionId())
                    .addArgument(Objects.requireNonNull(this.frontendHandler).remoteHost())
                    .addArgument(this.frontendHandler.remotePort())
                    .log();
            toClientActive(STARTING_STATE.toClientActive(), frontendHandler);
        }
        else {
            illegalState("Client activation while not in the start state");
        }
    }

    /**
     * Notify the statemachine that the connection to the backend has started.
     * @param peer the upstream host to connect to.
     */
    void onInitiateConnect(
                           HostPort peer) {
        if (state instanceof ProxyChannelState.SelectingServer selectingServerState) {
            toConnecting(selectingServerState.toConnecting(peer));
        }
        else {
            illegalState(DUPLICATE_INITIATE_CONNECT_ERROR);
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
    void illegalState(String msg) {
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
     * @param msg the RPC received from the downstream client
     */
    void onClientRequest(
                         Object msg) {
        Objects.requireNonNull(frontendHandler);
        if (state() instanceof Forwarding) { // post-backend connection
            messageFromClient(msg);
        }
        else if (!onClientRequestBeforeForwarding(msg)) {
            illegalState("Unexpected message received: " + (msg == null ? "null" : "message class=" + msg.getClass()));
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
        toClosed(null, DisconnectCause.SERVER_CLOSED);
    }

    /**
     * Notify the statemachine that the connection to the downstream client has been disconnected.
     * <p>
     * This will result in the proxy session being torn down.
     * </p>
     */
    void onClientInactive() {
        toClosed(null, DisconnectCause.CLIENT_CLOSED);
    }

    /**
     * Notify the statemachine that the connection to the downstream client is idle.
     * <p>
     * This will result in the proxy session being torn down.
     * </p>
     */
    void onClientIdle() {
        toClosed(null, DisconnectCause.IDLE_TIMEOUT);
    }

    /**
     * Notify the state machine that something exceptional and un-recoverable has happened on the upstream side.
     * @param cause the exception that triggered the issue
     */
    @SuppressWarnings("java:S5738")
    void onServerException(@Nullable Throwable cause) {
        LOGGER.atWarn()
                .setCause(LOGGER.isDebugEnabled() ? cause : null)
                .addArgument(cause != null ? cause.getMessage() : "")
                .log("Exception from the server channel: {}. Increase log level to DEBUG for stacktrace");
        proxyToServerErrorCounter.increment();
        toClosed(cause);
    }

    /**
     * Notify the state machine that something exceptional and un-recoverable has happened on the downstream side.
     * @param cause the exception that triggered the issue
     */
    @SuppressWarnings("java:S5738")
    void onClientException(@Nullable Throwable cause) {
        var tlsEnabled = endpointGateway().getDownstreamSslContext().isPresent();
        ApiException errorCodeEx;
        if (cause instanceof DecoderException de
                && de.getCause() instanceof FrameOversizedException e) {
            String tlsHint;
            tlsHint = tlsEnabled
                    ? ""
                    : " Possible unexpected TLS handshake? When connecting via TLS from your client, make sure to enable TLS for the Kroxylicious gateway ("
                            + StableKroxyliciousLinkGenerator.INSTANCE.errorLink(StableKroxyliciousLinkGenerator.CLIENT_TLS)
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
        clientToProxyErrorCounter.increment();
        toClosed(errorCodeEx);
    }

    /**
     * @return Return the session ID which connects a frontend channel with a backend channel
     */
    public String sessionId() {
        return kafkaSession.sessionId();
    }

    /**
     * @return Return the session for this connection.
     */
    public KafkaSession getKafkaSession() {
        return kafkaSession;
    }

    public void onSessionTransportAuthenticated() {
        this.kafkaSession.transitionTo(KafkaSessionState.TRANSPORT_AUTHENTICATED);
        Objects.requireNonNull(frontendHandler).onSessionAuthenticated();
    }

    public void onSessionSaslAuthenticated() {
        this.kafkaSession.transitionTo(KafkaSessionState.SASL_AUTHENTICATED);
        Objects.requireNonNull(frontendHandler).onSessionAuthenticated();
    }

    public Optional<ClientTlsContext> clientTlsContext() {
        return clientSubjectManager.clientTlsContext();
    }

    public void clientSaslAuthenticationSuccess(String mechanism, Subject subject) {
        clientSubjectManager.clientSaslAuthenticationSuccess(mechanism, subject);
    }

    public Optional<ClientSaslContext> clientSaslContext() {
        return clientSubjectManager.clientSaslContext();
    }

    public void clientSaslAuthenticationFailure() {
        clientSubjectManager.clientSaslAuthenticationFailure();
    }



    public void onClientTlsHandshakeSuccess(SSLSession sslSession) {
        this.clientSubjectManager.subjectFromTransport(sslSession, transportSubjectBuilder, this::onTransportSubjectBuilt);
    }

    @SuppressWarnings("java:S5738")
    private void toClientActive(
                                ProxyChannelState.ClientActive clientActive,
                                KafkaProxyFrontendHandler frontendHandler) {
        setState(clientActive);
        // we require two events before unblocking (making reads from) the client:
        // 1. the completion of the building of the transport subject
        // 2. the progression of the state machine to forwarding state
        //    (completion of the connection to the backend)
        // these can happen in either order
        this.progressionLatch = 2;
        if (!this.isTlsListener()) {
            this.clientSubjectManager.subjectFromTransport(null, this.transportSubjectBuilder, this::onTransportSubjectBuilt);
        }
        frontendHandler.inClientActive();

        clientToProxyConnectionCounter.increment();
        clientToProxyConnectionToken.acquire();
    }

    void onTransportSubjectBuilt() {
        if (!authenticatedSubject().isAnonymous()) {
            onSessionTransportAuthenticated();
        }
        maybeUnblock();
    }

    Subject authenticatedSubject() {
        return Objects.requireNonNull(clientSubjectManager).authenticatedSubject();
    }

    private void maybeUnblock() {
        if (--this.progressionLatch == 0) {
            Objects.requireNonNull(frontendHandler).unblockClient();
        }
    }

    @SuppressWarnings("java:S5738")
    private void toConnecting(
                              ProxyChannelState.Connecting connecting) {
        setState(connecting);
        backendHandler = new KafkaProxyBackendHandler(this);
        Objects.requireNonNull(frontendHandler).inConnecting(connecting.remote(), backendHandler);
        proxyToServerConnectionCounter.increment();
        LOGGER.atDebug()
                .setMessage("{}: Upstream connection to {} established for client at {}:{}")
                .addArgument(kafkaSession.sessionId())
                .addArgument(connecting.remote())
                .addArgument(Objects.requireNonNull(this.frontendHandler).remoteHost())
                .addArgument(this.frontendHandler.remotePort())
                .log();
    }

    @SuppressWarnings("java:S5738")
    private void toForwarding(Forwarding forwarding) {
        setState(forwarding);
        kafkaSession.transitionTo(KafkaSessionState.NOT_AUTHENTICATED);
        Objects.requireNonNull(frontendHandler).inForwarding();
        // once buffered message has been forwarded we enable auto-read to start accepting further messages
        maybeUnblock();
        proxyToServerConnectionToken.acquire();
    }

    /**
     * handle a message received from the client prior to connecting to the upstream node
     * @param msg Message received from the downstream client.
     * @return <code>false</code> for unsupported message types
     */
    private boolean onClientRequestBeforeForwarding(Object msg) {
        Objects.requireNonNull(frontendHandler).bufferMsg(msg);
        if (state() instanceof ProxyChannelState.ClientActive clientActive) {
            return onClientRequestInClientActiveState(msg, clientActive);
        }
        else if (state() instanceof ProxyChannelState.HaProxy haProxy) {
            return onClientRequestInHaProxyState(msg, haProxy);
        }
        else if (state() instanceof ProxyChannelState.SelectingServer) {
            return msg instanceof RequestFrame;
        }
        else {
            return state() instanceof ProxyChannelState.Connecting && msg instanceof RequestFrame;
        }
    }

    private boolean onClientRequestInHaProxyState(Object msg, ProxyChannelState.HaProxy haProxy) {
        return transitionClientRequest(msg, haProxy::toSelectingServer);
    }

    private boolean transitionClientRequest(
                                            Object msg,
                                            Function<DecodedRequestFrame<ApiVersionsRequestData>, ProxyChannelState.SelectingServer> selectingServerFactory) {
        if (isMessageApiVersionsRequest(msg)) {
            // We know it's an API Versions request even if the compiler doesn't
            @SuppressWarnings("unchecked")
            DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame = (DecodedRequestFrame<ApiVersionsRequestData>) msg;
            toSelectingServer(selectingServerFactory.apply(apiVersionsFrame));
            return true;
        }
        else if (msg instanceof RequestFrame) {
            toSelectingServer(selectingServerFactory.apply(null));
            return true;
        }
        return false;
    }

    private boolean onClientRequestInClientActiveState(Object msg, ProxyChannelState.ClientActive clientActive) {
        if (msg instanceof HAProxyMessage haProxyMessage) {
            toHaProxy(clientActive.toHaProxy(haProxyMessage));
            return true;
        }
        else {
            return transitionClientRequest(msg, clientActive::toSelectingServer);
        }
    }

    private void toHaProxy(ProxyChannelState.HaProxy haProxy) {
        setState(haProxy);
    }

    private void toSelectingServer(ProxyChannelState.SelectingServer selectingServer) {
        setState(selectingServer);
        Objects.requireNonNull(frontendHandler).inSelectingServer();
    }

    private void toClosed(@Nullable Throwable errorCodeEx) {
        toClosed(errorCodeEx, null);
    }

    private void toClosed(@Nullable Throwable errorCodeEx, @Nullable DisconnectCause disconnectCause) {
        if (state instanceof Closed) {
            return;
        }

        setState(new Closed());

        incrementAppropriateDisconnectsMetric(disconnectCause);

        kafkaSession.transitionTo(KafkaSessionState.TERMINATING);
        // Close the server connection
        if (backendHandler != null) {
            backendHandler.inClosed();
            proxyToServerConnectionToken.release();
        }

        // Close the client connection with any error code
        if (frontendHandler != null) { // Can be null if the error happens before clientActive (unlikely but possible)
            frontendHandler.inClosed(errorCodeEx);
            clientToProxyConnectionToken.release();
        }
    }

    private void incrementAppropriateDisconnectsMetric(@Nullable DisconnectCause disconnectCause) {
        // Increment disconnect counter based on cause (if not an error)
        if (disconnectCause != null) {
            switch (disconnectCause) {
                case IDLE_TIMEOUT:
                    clientToProxyDisconnectsIdleCounter.increment();
                    break;
                case CLIENT_CLOSED:
                    clientToProxyDisconnectsClientClosedCounter.increment();
                    break;
                case SERVER_CLOSED:
                    clientToProxyDisconnectsServerClosedCounter.increment();
                    break;
            }
        }
    }

    private void setState(ProxyChannelState state) {
        LOGGER.trace("{} transitioning to {}", this, state);
        this.state = state;
    }

    private static boolean isMessageApiVersionsRequest(Object msg) {
        return msg instanceof DecodedRequestFrame
                && ((DecodedRequestFrame<?>) msg).apiKey() == ApiKeys.API_VERSIONS;
    }

}
