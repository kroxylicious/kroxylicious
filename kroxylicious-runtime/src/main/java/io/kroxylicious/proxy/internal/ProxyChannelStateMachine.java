/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.net.ssl.SSLSession;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.util.ReferenceCountUtil;

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
 *  ╭───┤
 *  │   ↓ {@link #onDraining(Runnable) onDraining}
 *  │  {@link ProxyChannelState.Draining Draining} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *  │   │ {@link #onDrainCompleted() onDrainCompleted} (drained naturally)
 *  │   │ or {@link #onDrainTimeout() onDrainTimeout} (force-closed after timeout)
 *  ╰───┤
 *      ↓ backend.{@link KafkaProxyBackendHandler#channelInactive(ChannelHandlerContext) channelInactive}
 *      ↓ or frontend.{@link KafkaProxyFrontendHandler#channelInactive(ChannelHandlerContext) channelInactive}
 *     {@link Closed Closed} ⇠╌╌╌╌ <b>error</b> ⇠╌╌╌╌
 * </pre>
 *
 * <p>The {@link ProxyChannelState.Draining Draining} state is optional: a connection only enters it
 * when {@link #initiateClose(Duration)} is invoked externally (typically by the {@code DrainCoordinator}
 * during proxy shutdown or virtual-cluster hot-reload). The {@code on*} methods that perform the actual
 * state transitions ({@code onDraining}, {@code onDrainCompleted}, {@code onDrainTimeout}) are private
 * and orchestrated internally by {@code initiateClose}. Any {@code channelInactive} or error event that
 * arrives while in {@code Draining} routes through {@link #toClosed} the same way it would from
 * {@code Forwarding}; the merged-edge label applies to both paths.</p>
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
 *     When either side of the proxy starts applying back pressure the proxy should propagate that fact to the other peer.
 *     Thus, when the proxy is notified that a peer is applying back pressure it results in action on the channel with the opposite peer.
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
        SERVER_CLOSED("server_closed"),
        /** Connection closed after graceful drain completed (all in-flight requests responded) */
        DRAIN_COMPLETED("drain_completed"),
        /** Connection force-closed after drain timeout expired */
        DRAIN_TIMEOUT("drain_timeout");

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
    private final Counter clientToProxyDisconnectsDrainCompletedCounter;
    private final Counter clientToProxyDisconnectsDrainTimeoutCounter;
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

    private final DrainCoordinator drainCoordinator;
    /** Tracks requests sent to the server that haven't received a response yet (proxy↔server). */
    private int serverMessagesInFlightCount;
    /** Tracks requests received from the client whose response hasn't been forwarded back yet (client↔proxy). */
    private int clientMessagesInFlightCount;

    public ProxyChannelStateMachine(EndpointBinding endpointBinding,
                                    TransportSubjectBuilder transportSubjectBuilder,
                                    KafkaSession kafkaSession,
                                    DrainCoordinator drainCoordinator) {
        this.endpointBinding = endpointBinding;
        this.transportSubjectBuilder = transportSubjectBuilder;
        this.drainCoordinator = Objects.requireNonNull(drainCoordinator);
        this.kafkaSession = kafkaSession;
        var virtualCluster = endpointBinding.endpointGateway().virtualCluster();

        var nodeId = endpointBinding.nodeId();
        String clusterName = virtualCluster.getClusterName();
        VirtualClusterNode node = new VirtualClusterNode(clusterName, nodeId);
        // Connection metrics
        clientToProxyConnectionCounter = Metrics.clientToProxyConnectionCounter(clusterName, nodeId).withTags();
        clientToProxyDisconnectsIdleCounter = Metrics.clientToProxyDisconnectsCounter(clusterName, nodeId, DisconnectCause.IDLE_TIMEOUT.label()).withTags();
        clientToProxyDisconnectsClientClosedCounter = Metrics.clientToProxyDisconnectsCounter(clusterName, nodeId, DisconnectCause.CLIENT_CLOSED.label()).withTags();
        clientToProxyDisconnectsServerClosedCounter = Metrics.clientToProxyDisconnectsCounter(clusterName, nodeId, DisconnectCause.SERVER_CLOSED.label()).withTags();
        clientToProxyDisconnectsDrainCompletedCounter = Metrics.clientToProxyDisconnectsCounter(clusterName, nodeId, DisconnectCause.DRAIN_COMPLETED.label()).withTags();
        clientToProxyDisconnectsDrainTimeoutCounter = Metrics.clientToProxyDisconnectsCounter(clusterName, nodeId, DisconnectCause.DRAIN_TIMEOUT.label()).withTags();
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
    void forceState(ProxyChannelState state,
                    KafkaProxyFrontendHandler frontendHandler,
                    @Nullable KafkaProxyBackendHandler backendHandler,
                    KafkaSession kafkaSession,
                    int transportAndBackendLatch) {
        LOGGER.atInfo()
                .addKeyValue("state", state)
                .addKeyValue("frontendHandler", frontendHandler)
                .addKeyValue("backendHandler", backendHandler)
                .log("Forcing state");
        this.state = state;
        this.kafkaSession = kafkaSession;
        this.frontendHandler = frontendHandler;
        this.backendHandler = backendHandler;
        this.progressionLatch = transportAndBackendLatch;
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
                    .addKeyValue("sessionId", kafkaSession.sessionId())
                    .addKeyValue("remoteHost", Objects.requireNonNull(this.frontendHandler).remoteHost())
                    .addKeyValue("remotePort", this.frontendHandler.remotePort())
                    .log("Allocated session ID for downstream connection");
            ProxyChannelState.ClientActive clientActive = STARTING_STATE.toClientActive();
            toClientActive(clientActive, frontendHandler);
            // If an HaProxy context was stored in KafkaSession (by the detection/message
            // handlers before PCSM was created), transition ClientActive → HaProxy.
            if (kafkaSession.haProxyContext() != null) {
                toHaProxy(clientActive.toHaProxy());
            }
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
            LOGGER.atError()
                    .addKeyValue("state", state)
                    .addKeyValue("message", msg)
                    .log("Unexpected event, closing channels with no client response");
            toClosed(null);
        }
    }

    /**
     * A message has been received from the upstream node which should be passed to the downstream client
     * @param msg the object received from the upstream
     */
    void messageFromServer(Object msg) {
        // Decrement server-side counter: response received from Kafka
        serverMessagesInFlightCount = Math.max(0, serverMessagesInFlightCount - 1);

        // Forward response to client (goes through filter pipeline)
        Objects.requireNonNull(frontendHandler).forwardToClient(msg);

        // Decrement client-side counter: response delivered to client
        clientMessagesInFlightCount = Math.max(0, clientMessagesInFlightCount - 1);

        if (state instanceof ProxyChannelState.Draining draining) {
            if (clientMessagesInFlightCount <= 0) {
                LOGGER.atInfo()
                        .addKeyValue("sessionId", kafkaSession.sessionId())
                        .addKeyValue("virtualCluster", clusterName())
                        .addKeyValue("frontendChannel", () -> frontendChannelAddress())
                        .addKeyValue("backendChannel", () -> backendChannelAddress())
                        .log("All in-flight requests drained — signalling drain policy");
                draining.onDrained().run();
            }
            else {
                LOGGER.atTrace()
                        .addKeyValue("sessionId", kafkaSession.sessionId())
                        .addKeyValue("virtualCluster", clusterName())
                        .addKeyValue("frontendChannel", () -> frontendChannelAddress())
                        .addKeyValue("backendChannel", () -> backendChannelAddress())
                        .addKeyValue("clientMessagesInFlightCount", clientMessagesInFlightCount)
                        .log("Response delivered to client during drain — still waiting for remaining in-flight requests");
            }
        }
    }

    /**
     * Called to notify the state machine that reading the upstream batch is complete.
     */
    void serverReadComplete() {
        Objects.requireNonNull(frontendHandler).flushToClient();
    }

    /**
     * A message has emerged from the Filter Chain and is ready to be forwarded to the upstream node.
     * <p>
     * This path is reachable in either {@link Forwarding} or {@link ProxyChannelState.Draining} —
     * the filter chain is asynchronous, so a request that entered the chain while we were
     * Forwarding may emerge after we transitioned to Draining. We must still forward such
     * requests to the broker: drain promises to <em>complete</em> in-flight work, not drop it.
     * autoRead is disabled on entry to Draining, so no NEW requests can join the filter chain
     * after that point — the only messages reaching here in Draining are ones already mid-flight.
     *
     * @param msg the RPC received from the upstream
     */
    void onClientFilterChainComplete(Object msg) {
        if (state() instanceof Forwarding || state() instanceof ProxyChannelState.Draining) {
            // Increment server-side counter: request being forwarded to Kafka.
            serverMessagesInFlightCount++;
            Objects.requireNonNull(backendHandler).forwardToServer(msg);
            backendHandler.flushToServer();
        }
        else {
            illegalState("Unexpected message received: " + (msg == null ? "null" : "message class=" + msg.getClass()));
        }
    }

    /**
     * The proxy has received something from the client. The current state of the session determines what happens to it.
     * @param msg the RPC received from the downstream client
     */

    void onClientRequest(
                         Object msg) {
        Objects.requireNonNull(frontendHandler);
        // Count every msg received from the client.
        clientMessagesInFlightCount++;
        if (state() instanceof Forwarding) { // post-backend connection
            frontendHandler.admitToFilterChain(msg);
        }
        else if (state() instanceof ProxyChannelState.Draining draining) {
            // autoRead is disabled the moment we enter Draining, so the only frames that can
            // still land here are ones Netty had already buffered/decoded before that took
            // effect. Dropping them is the simplest behaviour for the current callers (proxy
            // shutdown, VC hot-reload), which are tearing the connection down imminently
            // anyway.
            ReferenceCountUtil.release(msg);
            if (msg instanceof RequestFrame) {
                clientMessagesInFlightCount = Math.max(0, clientMessagesInFlightCount - 1);
                if (clientMessagesInFlightCount <= 0) {
                    draining.onDrained().run();
                }
            }
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
     * Begin draining this connection and return a future that completes once the connection has
     * fully closed — either naturally (all in-flight responses delivered) or after the
     * {@code timeout} force-closes it. Safe to call from any thread; orchestration is dispatched
     * onto the channel's event loop.
     * <p>
     * Mechanics:
     * <ul>
     *   <li>Schedules a force-close timer that invokes {@link #onDrainTimeout()} after
     *       {@code timeout}.</li>
     *   <li>Dispatches {@link #onDraining(Runnable)} onto the event loop, injecting a policy
     *       that cancels the timer, completes the returned future, and invokes
     *       {@link #onDrainCompleted()} (transitioning the PCSM to {@link Closed} with
     *       {@code DRAIN_COMPLETED}).</li>
     * </ul>
     * The injected policy is idempotent: cancellation and future completion are no-ops once
     * already invoked, and {@code onDrainCompleted} is a no-op when the state has already
     * transitioned away from {@link ProxyChannelState.Draining}. This lets the three drain-
     * termination paths (natural completion, timeout, orphan close via {@link #toClosed})
     * converge on the same future without double-firing.
     *
     * @param timeout maximum time to wait for in-flight responses before force-closing
     * @return future that completes when this connection has reached {@link Closed}
     */
    CompletableFuture<Void> initiateClose(Duration timeout) {
        CompletableFuture<Void> closedFuture = new CompletableFuture<>();

        ScheduledFuture<?> timeoutTask = scheduleOnEventLoop(this::onDrainTimeout, timeout);

        Runnable onDrained = () -> {
            timeoutTask.cancel(false);
            closedFuture.complete(null);
            onDrainCompleted();
        };

        executeOnEventLoop(() -> onDraining(onDrained));
        return closedFuture;
    }

    /**
     * Begin draining: disable autoRead on the downstream channel and transition to
     * {@link ProxyChannelState.Draining}, carrying the injected {@code onDrained} policy.
     * If no requests are already in-flight, the policy fires immediately.
     * <p>
     * Internal: invoked only from {@link #initiateClose(Duration)}, which dispatches it
     * onto the event loop. Must run on the channel's event loop thread.
     *
     * @param onDrained policy to invoke when the in-flight counter reaches zero; responsible
     *                  for closing the connection (via {@link #onDrainCompleted()}) and any
     *                  bookkeeping (future completion, timer cancellation)
     */
    private void onDraining(Runnable onDrained) {
        if (!(state instanceof Forwarding)) {
            LOGGER.atWarn()
                    .addKeyValue("sessionId", kafkaSession.sessionId())
                    .addKeyValue("virtualCluster", clusterName())
                    .addKeyValue("state", state.getClass().getSimpleName())
                    .log("Cannot start draining — not in Forwarding state");
            onDrained.run();
            return;
        }

        // Disable downstream reads — no new requests from client
        Objects.requireNonNull(frontendHandler).applyBackpressure();

        setState(new ProxyChannelState.Draining(onDrained));
        LOGGER.atInfo()
                .addKeyValue("sessionId", kafkaSession.sessionId())
                .addKeyValue("virtualCluster", clusterName())
                .addKeyValue("frontendChannel", () -> frontendChannelAddress())
                .addKeyValue("backendChannel", () -> backendChannelAddress())
                .addKeyValue("clientMessagesInFlightCount", clientMessagesInFlightCount)
                .addKeyValue("serverMessagesInFlightCount", serverMessagesInFlightCount)
                .log("Connection draining started — autoRead disabled, waiting for in-flight responses");

        if (clientMessagesInFlightCount <= 0) {
            LOGGER.atInfo()
                    .addKeyValue("sessionId", kafkaSession.sessionId())
                    .addKeyValue("virtualCluster", clusterName())
                    .addKeyValue("frontendChannel", () -> frontendChannelAddress())
                    .addKeyValue("backendChannel", () -> backendChannelAddress())
                    .log("No in-flight requests — signalling drain policy immediately");
            onDrained.run();
        }
    }

    /**
     * Drain completed naturally: invoked by the {@code onDrained} policy when the in-flight
     * counter reaches zero, transitioning the connection to {@link Closed} with the
     * {@code DRAIN_COMPLETED} cause for metrics. No-op if the state has already transitioned
     * away from {@link ProxyChannelState.Draining}.
     * <p>
     * Internal: only called from the policy assembled in {@link #initiateClose(Duration)}.
     */
    private void onDrainCompleted() {
        if (state instanceof ProxyChannelState.Draining) {
            toClosed(null, DisconnectCause.DRAIN_COMPLETED);
        }
    }

    /**
     * The drain timeout timer expired. Force-closes the connection with the {@code DRAIN_TIMEOUT}
     * cause for metrics. No-op if the state has already transitioned away from
     * {@link ProxyChannelState.Draining} (e.g. drain already completed or the connection closed
     * for another reason).
     * <p>
     * Internal: only invoked by the timer scheduled in {@link #initiateClose(Duration)}.
     */
    private void onDrainTimeout() {
        if (state instanceof ProxyChannelState.Draining) {
            LOGGER.atWarn()
                    .addKeyValue("sessionId", kafkaSession.sessionId())
                    .addKeyValue("virtualCluster", clusterName())
                    .addKeyValue("frontendChannel", () -> frontendChannelAddress())
                    .addKeyValue("backendChannel", () -> backendChannelAddress())
                    .addKeyValue("clientMessagesInFlightCount", clientMessagesInFlightCount)
                    .log("Drain timeout expired — force-closing connection");
            toClosed(null, DisconnectCause.DRAIN_TIMEOUT);
        }
    }

    /**
     * Notify the state machine that something exceptional and un-recoverable has happened on the upstream side.
     * @param cause the exception that triggered the issue
     */
    @SuppressWarnings("java:S5738")
    void onServerException(@Nullable Throwable cause) {
        LOGGER.atWarn()
                .addKeyValue("error", cause != null ? cause.getMessage() : "")
                .setCause(LOGGER.isDebugEnabled() ? cause : null)
                .log(LOGGER.isDebugEnabled()
                        ? "exception from server channel"
                        : "exception from server channel, increase log level to DEBUG for stacktrace");
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
            LOGGER.atWarn()
                    .addKeyValue("maxFrameSizeBytes", e.getMaxFrameSizeBytes())
                    .addKeyValue("receivedFrameSizeBytes", e.getReceivedFrameSizeBytes())
                    .addKeyValue("hint", tlsHint)
                    .log("Received over-sized frame from client, other possible causes are: an oversized Kafka frame, or something unexpected like an HTTP request");
            errorCodeEx = Errors.INVALID_REQUEST.exception();
        }
        else {
            LOGGER.atWarn()
                    .addKeyValue("error", cause != null ? cause.getMessage() : "")
                    .setCause(LOGGER.isDebugEnabled() ? cause : null)
                    .log(LOGGER.isDebugEnabled()
                            ? "exception from client channel"
                            : "exception from client channel, increase log level to DEBUG for stacktrace");
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
    public KafkaSession kafkaSession() {
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
        this.clientSubjectManager.subjectFromTransport(sslSession, transportSubjectBuilder,
                Objects.requireNonNull(frontendHandler).eventLoopExecutor(), this::onTransportSubjectBuilt);
    }

    @SuppressWarnings("java:S5738")
    private void toClientActive(
                                ProxyChannelState.ClientActive clientActive,
                                KafkaProxyFrontendHandler frontendHandler) {
        setState(clientActive);
        // we require two events before unblocking (making reads from) the client:
        // 1. the completion of the building of the transport subject
        // 2. the progression of the state machine to forwarding state
        // (completion of the connection to the backend)
        // these can happen in either order
        this.progressionLatch = 2;
        if (!this.isTlsListener()) {
            this.clientSubjectManager.subjectFromTransport(null, this.transportSubjectBuilder,
                    frontendHandler.eventLoopExecutor(), this::onTransportSubjectBuilt);
        }
        frontendHandler.inClientActive();

        clientToProxyConnectionCounter.increment();
        clientToProxyConnectionToken.acquire();

        drainCoordinator.register(clusterName(), this);
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
                .addKeyValue("sessionId", kafkaSession.sessionId())
                .addKeyValue("remote", connecting.remote())
                .addKeyValue("clientHost", Objects.requireNonNull(this.frontendHandler).remoteHost())
                .addKeyValue("clientPort", this.frontendHandler.remotePort())
                .log("Upstream connection established for client");
    }

    @SuppressWarnings("java:S5738")
    private void toForwarding(Forwarding forwarding) {
        setState(forwarding);
        kafkaSession.transitionTo(KafkaSessionState.NOT_AUTHENTICATED);
        // we must wait for the transport subject to be built before forwarding the buffered messages and then enabling autoread on the client
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
        return transitionClientRequest(msg, clientActive::toSelectingServer);
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

        // Capture the drain-completion callback when transitioning out of Draining for
        // reasons OTHER than natural drain completion. The DRAIN_COMPLETED path reaches
        // toClosed from inside the callback itself (messageFromServer → onDrained → pcsm.onDrainCompleted
        // → toClosed) — re-firing here would invoke the callback twice. For any other cause
        // (DRAIN_TIMEOUT, orphan server/client errors during drain) the callback has NOT run yet
        // and must fire so the coordinator's future unblocks and its timer cancels.
        Runnable pendingDrainCallback = (state instanceof ProxyChannelState.Draining draining
                && disconnectCause != DisconnectCause.DRAIN_COMPLETED) ? draining.onDrained() : null;

        setState(new Closed());

        incrementAppropriateDisconnectsMetric(disconnectCause);

        kafkaSession.transitionTo(KafkaSessionState.TERMINATING);
        // Close the server connection
        if (backendHandler != null) {
            backendHandler.inClosed();
            proxyToServerConnectionToken.release();
        }

        // Close the client connection
        if (frontendHandler != null) { // Can be null if the error happens before clientActive (unlikely but possible)
            frontendHandler.inClosed(errorCodeEx);
            clientToProxyConnectionToken.release();
        }

        // Deregister from drain coordinator
        drainCoordinator.deregister(clusterName(), this);

        // Fire the drain policy if we were draining when we entered toClosed — signals the
        // coordinator that this connection is closed regardless of whether drain completed
        // naturally or the connection was torn down for another reason.
        if (pendingDrainCallback != null) {
            LOGGER.atInfo()
                    .addKeyValue("sessionId", kafkaSession.sessionId())
                    .addKeyValue("virtualCluster", clusterName())
                    .addKeyValue("frontendChannel", () -> frontendChannelAddress())
                    .addKeyValue("backendChannel", () -> backendChannelAddress())
                    .addKeyValue("disconnectCause", disconnectCause)
                    .addKeyValue("errorCodeEx", errorCodeEx == null ? null : errorCodeEx.getClass().getSimpleName() + ": " + errorCodeEx.getMessage())
                    .addKeyValue("clientMessagesInFlightCount", clientMessagesInFlightCount)
                    .addKeyValue("serverMessagesInFlightCount", serverMessagesInFlightCount)
                    .log("Drain interrupted by connection close — signalling drain policy from toClosed path");
            pendingDrainCallback.run();
        }
    }

    private void incrementAppropriateDisconnectsMetric(@Nullable DisconnectCause disconnectCause) {
        // Increment disconnect counter based on cause (if not an error)
        if (disconnectCause != null) {
            switch (disconnectCause) {
                case IDLE_TIMEOUT -> clientToProxyDisconnectsIdleCounter.increment();
                case CLIENT_CLOSED -> clientToProxyDisconnectsClientClosedCounter.increment();
                case SERVER_CLOSED -> clientToProxyDisconnectsServerClosedCounter.increment();
                case DRAIN_COMPLETED -> clientToProxyDisconnectsDrainCompletedCounter.increment();
                case DRAIN_TIMEOUT -> clientToProxyDisconnectsDrainTimeoutCounter.increment();
            }
        }
    }

    private void setState(ProxyChannelState state) {
        LOGGER.atTrace()
                .addKeyValue("stateMachine", this)
                .addKeyValue("targetState", state)
                .log("Transitioning to state");
        this.state = state;
    }

    private static boolean isMessageApiVersionsRequest(Object msg) {
        return msg instanceof DecodedRequestFrame
                && ((DecodedRequestFrame<?>) msg).apiKey() == ApiKeys.API_VERSIONS;
    }

    /**
     * Dispatch a task onto this PCSM's event loop. Used internally so that {@code on*()}
     * transition methods always run on the channel's event loop thread, regardless of which
     * thread {@link #initiateClose(Duration)} is invoked from.
     * @throws IllegalStateException if the PCSM has no frontend channel attached yet
     */
    private void executeOnEventLoop(Runnable task) {
        requireFrontendChannel().eventLoop().execute(task);
    }

    /**
     * Schedule a delayed task onto this PCSM's event loop. Returns the scheduled-future handle
     * so callers can cancel it. Used internally to schedule the drain force-close timer.
     * @throws IllegalStateException if the PCSM has no frontend channel attached yet
     */
    private ScheduledFuture<?> scheduleOnEventLoop(Runnable task, Duration delay) {
        return requireFrontendChannel().eventLoop().schedule(task, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private Channel requireFrontendChannel() {
        Channel ch = frontendHandler != null ? frontendHandler.clientChannel() : null;
        if (ch == null) {
            throw new IllegalStateException("PCSM has no frontend channel attached — dispatch not possible in state " + state.getClass().getSimpleName());
        }
        return ch;
    }

    private String frontendChannelAddress() {
        Channel ch = frontendHandler != null ? frontendHandler.clientChannel() : null;
        if (ch == null) {
            return "unknown";
        }
        return "L:" + ch.localAddress() + ", R:" + ch.remoteAddress();
    }

    private String backendChannelAddress() {
        if (backendHandler == null) {
            return "unknown";
        }
        Channel ch = backendHandler.serverChannel();
        if (ch == null) {
            return "unknown";
        }
        return "L:" + ch.localAddress() + ", R:" + ch.remoteAddress();
    }

}
