/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.net.ssl.SSLSession;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.event.Level;
import org.slf4j.spi.LoggingEventBuilder;

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
import io.kroxylicious.proxy.internal.ClientConnectionState.Closed;
import io.kroxylicious.proxy.internal.ClientConnectionState.Forwarding;
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.routing.RouteDescriptor;
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

import static io.kroxylicious.proxy.internal.ClientConnectionState.Startup.STARTING_STATE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * <p>The state machine for a single client's proxy session.
 * The "session state" is held in the {@link #state} field and is represented by an immutable
 * subclass of {@link ClientConnectionState} which contains state-specific data.
 * Events which cause state transitions are represented by the {@code on*()} family of methods.
 * Depending on the transition the frontend or backend handlers may get notified via one if their
 * {@code in*()} methods.
 * </p>
 *
 * <pre>
 *   «start»
 *      │
 *      ↓ frontend.{@link KafkaProxyFrontendHandler#channelActive(ChannelHandlerContext) channelActive}
 *     {@link ClientConnectionState.ClientActive ClientActive} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *  ╭───┤
 *  ↓   ↓ frontend.{@link KafkaProxyFrontendHandler#channelRead(ChannelHandlerContext, Object) channelRead} receives a PROXY header
 *  │  {@link ClientConnectionState.HaProxy HaProxy} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *  ╰───┤
 *      ↓ frontend.{@link KafkaProxyFrontendHandler#channelRead(ChannelHandlerContext, Object) channelRead} receives any KRPC request
 *     {@link Forwarding Forwarding} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *  ╭───┤
 *  │   ↓ {@link #onDraining(Runnable, CompletableFuture) onDraining}
 *  │  {@link ClientConnectionState.Draining Draining} ╌╌╌╌⤍ <b>error</b> ╌╌╌╌⤍
 *  │   │ {@link #onDrainCompleted() onDrainCompleted} (drained naturally)
 *  │   │ or {@link #onDrainTimeout() onDrainTimeout} (force-closed after timeout)
 *  ╰───┤
 *      ↓ backend.{@link KafkaProxyBackendHandler#channelInactive(ChannelHandlerContext) channelInactive}
 *      ↓ or frontend.{@link KafkaProxyFrontendHandler#channelInactive(ChannelHandlerContext) channelInactive}
 *     {@link Closed Closed} ⇠╌╌╌╌ <b>error</b> ⇠╌╌╌╌
 * </pre>
 *
 * <p>The {@link ClientConnectionState.Draining Draining} state is optional: a connection only enters it
 * when {@link #drain(Duration)} is invoked externally (typically by {@code VirtualClusterLifecycle}
 * during proxy shutdown or virtual-cluster hot-reload). The {@code on*} methods that perform the actual
 * state transitions ({@code onDraining}, {@code onDrainCompleted}, {@code onDrainTimeout}) are private
 * and orchestrated internally by {@code drain}. Any {@code channelInactive} or error event that
 * arrives while in {@code Draining} routes through {@link #toClosed} the same way it would from
 * {@code Forwarding}; the merged-edge label applies to both paths.</p>
 *
 * <p>In addition to the "session state" this class manages the client-side of TCP backpressure
 * via the {@link #clientReadsBlocked} field. Server-side backpressure is managed by the
 * {@link ServerConnectionStateMachine}.</p>
 *
 * <p>
 *     When either side of the proxy starts applying back pressure the proxy should propagate that fact to the other peer.
 *     Thus, when the proxy is notified that a peer is applying back pressure it results in action on the channel with the opposite peer.
 * </p>
 */
@SuppressWarnings({ "java:S1133", "java:S1172" }) // S1172: scsm params on ServerConnectionStateMachine callbacks identify the caller for multi-backend routing
public class ClientConnectionStateMachine {
    private static final Logger LOGGER = getLogger(ClientConnectionStateMachine.class);

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

    // Client-side connection metrics
    private final Counter clientToProxyErrorCounter;
    private final Counter clientToProxyDisconnectsIdleCounter;
    private final Counter clientToProxyDisconnectsClientClosedCounter;
    private final Counter clientToProxyDisconnectsServerClosedCounter;
    private final Counter clientToProxyDisconnectsDrainCompletedCounter;
    private final Counter clientToProxyDisconnectsDrainTimeoutCounter;
    private final Counter clientToProxyConnectionCounter;
    private final Timer clientToProxyBackPressureMeter;

    private final ActivationToken clientToProxyConnectionToken;

    // Server-side metrics (passed to SCSM at construction)
    private final Counter proxyToServerConnectionCounter;
    private final Counter proxyToServerErrorCounter;
    private final Timer serverToProxyBackpressureMeter;
    private final ActivationToken proxyToServerConnectionToken;

    @VisibleForTesting
    @Nullable
    Timer.Sample clientToProxyBackpressureTimer;
    private final ServerConnectionFactory serverConnectionFactory;

    private final EndpointBinding endpointBinding;

    @NonNull
    // Ideally this would be final, however that breaks forceState which is used heavily in testing.
    private KafkaSession kafkaSession;

    /**
     * The current state. This can be changed via a call to one of the {@code on*()} methods.
     */
    private ClientConnectionState state = STARTING_STATE;

    /*
     * The netty autoread flag is volatile =>
     * expensive to set in every call to channelRead.
     * So we track autoread states via these non-volatile fields,
     * allowing us to only touch the volatile when it needs to be changed
     */
    @VisibleForTesting
    boolean clientReadsBlocked;
    private final TransportSubjectBuilder transportSubjectBuilder;
    private final ClientSubjectManager clientSubjectManager = new ClientSubjectManager();
    private boolean transportSubjectReady;
    /**
     * The frontend handler. Non-null if we got as far as ClientActive.
     */
    @SuppressWarnings({ "java:S2637" })
    private @Nullable KafkaProxyFrontendHandler frontendHandler = null;

    /**
     * Server connection state machines, keyed by remote address. Populated when the first client
     * request triggers backend connection setup (transition to {@link Forwarding}). Currently
     * contains at most one entry; routing will add more.
     */
    @VisibleForTesting
    final Map<HostPort, ServerConnectionStateMachine> serverConnections = new HashMap<>();

    @Nullable
    private String clientSoftwareName;
    @Nullable
    private String clientSoftwareVersion;

    /** Tracks requests received from the client whose response hasn't been forwarded back yet (client↔proxy). */
    private int clientMessagesInFlightCount;

    @Nullable
    private Map<String, HostPort> routeTargets;

    public ClientConnectionStateMachine(EndpointBinding endpointBinding,
                                        TransportSubjectBuilder transportSubjectBuilder,
                                        KafkaSession kafkaSession) {
        this(endpointBinding, transportSubjectBuilder, kafkaSession, ServerConnectionStateMachine::new);
    }

    @VisibleForTesting
    ClientConnectionStateMachine(EndpointBinding endpointBinding,
                                 TransportSubjectBuilder transportSubjectBuilder,
                                 KafkaSession kafkaSession,
                                 ServerConnectionFactory serverConnectionFactory) {
        this.endpointBinding = endpointBinding;
        this.transportSubjectBuilder = transportSubjectBuilder;
        this.kafkaSession = kafkaSession;
        this.serverConnectionFactory = serverConnectionFactory;
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

    ClientConnectionState state() {
        return state;
    }

    /**
     * Purely for tests DO NOT USE IN PRODUCTION code!!
     * Sonar will complain if one uses this in prod code listen to it.
     */
    @VisibleForTesting
    void forceState(ClientConnectionState state,
                    KafkaProxyFrontendHandler frontendHandler,
                    Map<HostPort, ServerConnectionStateMachine> serverConnections,
                    KafkaSession kafkaSession,
                    boolean transportSubjectReady) {
        forceState(state, frontendHandler, serverConnections, kafkaSession, transportSubjectReady, null);
    }

    @VisibleForTesting
    void forceState(ClientConnectionState state,
                    KafkaProxyFrontendHandler frontendHandler,
                    Map<HostPort, ServerConnectionStateMachine> serverConnections,
                    KafkaSession kafkaSession,
                    boolean transportSubjectReady,
                    @Nullable Map<String, HostPort> routeTargets) {
        LOGGER.atInfo()
                .addKeyValue("sessionId", kafkaSession.sessionId())
                .addKeyValue("virtualCluster", clusterName())
                .addKeyValue("state", state)
                .addKeyValue("frontendHandler", frontendHandler)
                .addKeyValue("serverConnections", serverConnections)
                .log("Forcing state");
        this.state = state;
        this.kafkaSession = kafkaSession;
        this.frontendHandler = frontendHandler;
        this.serverConnections.clear();
        this.serverConnections.putAll(serverConnections);
        this.transportSubjectReady = transportSubjectReady;
        this.routeTargets = routeTargets;
    }

    @Override
    public String toString() {
        return "StateHolder{" +
                "state=" + state +
                ", clientReadsBlocked=" + clientReadsBlocked +
                ", frontendHandler=" + frontendHandler +
                ", serverConnections=" + serverConnections +
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

    @NonNull
    VirtualClusterModel virtualCluster() {
        return endpointBinding.endpointGateway().virtualCluster();
    }

    boolean isTlsListener() {
        return endpointBinding.endpointGateway().isUseTls();
    }

    @Nullable
    String clientSoftwareName() {
        return clientSoftwareName;
    }

    @Nullable
    String clientSoftwareVersion() {
        return clientSoftwareVersion;
    }

    @Nullable
    public Channel clientChannel() {
        return frontendHandler != null ? frontendHandler.clientChannel() : null;
    }

    /**
     * Notify the state machine when the client applies back pressure.
     */
    public void onClientUnwritable() {
        for (ServerConnectionStateMachine scsm : serverConnections.values()) {
            scsm.applyBackpressure();
        }
    }

    /**
     * Notify the state machine when the client stops applying back pressure
     */
    public void onClientWritable() {
        for (ServerConnectionStateMachine scsm : serverConnections.values()) {
            scsm.relieveBackpressure();
        }
    }

    /**
     * Notify the state machine when the server applies back pressure
     */
    void onServerUnwritable(ServerConnectionStateMachine scsm) {
        if (!clientReadsBlocked) {
            clientReadsBlocked = true;
            clientToProxyBackpressureTimer = Timer.start();
            Objects.requireNonNull(frontendHandler).applyBackpressure();
        }
    }

    /**
     * Notify the state machine when the server stops applying back pressure
     */
    void onServerWritable(ServerConnectionStateMachine scsm) {
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
            log(Level.DEBUG)
                    .addKeyValue("address", () -> HostPort.asString(frontendHandler.remoteHost(), frontendHandler.remotePort()))
                    .log("Allocated session ID for downstream connection");
            ClientConnectionState.ClientActive clientActive = STARTING_STATE.toClientActive();
            toClientActive(clientActive, frontendHandler);
            // If an HaProxy context was stored in KafkaSession (by the detection/message
            // handlers before CCSM was created), transition ClientActive → HaProxy.
            if (kafkaSession.haProxyContext() != null) {
                toHaProxy(clientActive.toHaProxy());
            }
        }
        else {
            illegalState("Client activation while not in the start state");
        }
    }

    /**
     * Callback from {@link ServerConnectionStateMachine} when the upstream connection is ready for RPC calls.
     */
    void onServerConnectionActive(ServerConnectionStateMachine scsm) {
        if (state() instanceof Forwarding) {
            kafkaSession.transitionTo(KafkaSessionState.NOT_AUTHENTICATED);
        }
        else {
            illegalState("Server became active while not in the Forwarding state");
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
            log(Level.ERROR)
                    .addKeyValue("state", state)
                    .addKeyValue("message", msg)
                    .log("Unexpected event, closing channels with no client response");
            toClosed(null);
        }
    }

    /**
     * Callback from {@link ServerConnectionStateMachine} when a response has been received from the
     * upstream node and should be passed to the downstream client.
     * @param msg the object received from the upstream
     */
    void onResponseFromServer(ServerConnectionStateMachine scsm,
                              Object msg) {
        Objects.requireNonNull(frontendHandler).forwardToClient(msg);

        clientMessagesInFlightCount = Math.max(0, clientMessagesInFlightCount - 1);

        if (state instanceof ClientConnectionState.Draining draining) {
            if (clientMessagesInFlightCount <= 0) {
                LOGGER.atInfo()
                        .addKeyValue("sessionId", kafkaSession.sessionId())
                        .addKeyValue("virtualCluster", clusterName())
                        .log("All in-flight requests drained — signalling drain policy");
                draining.onDrained().run();
            }
            else {
                LOGGER.atTrace()
                        .addKeyValue("sessionId", kafkaSession.sessionId())
                        .addKeyValue("virtualCluster", clusterName())
                        .addKeyValue("clientMessagesInFlightCount", clientMessagesInFlightCount)
                        .log("Response delivered to client during drain — still waiting for remaining in-flight requests");
            }
        }
    }

    /**
     * Callback from {@link ServerConnectionStateMachine} when reading the upstream batch is complete.
     */
    void onServerReadComplete(ServerConnectionStateMachine scsm) {
        Objects.requireNonNull(frontendHandler).flushToClient();
    }

    /**
     * A message has emerged from the Filter Chain and is ready to be forwarded to the upstream node.
     * <p>
     * This path is reachable in either {@link Forwarding} or {@link ClientConnectionState.Draining} —
     * the filter chain is asynchronous, so a request that entered the chain while we were
     * Forwarding may emerge after we transitioned to Draining. We must still forward such
     * requests to the broker: drain promises to <em>complete</em> in-flight work, not drop it.
     * autoRead is disabled on entry to Draining, so no NEW requests can join the filter chain
     * after that point — the only messages reaching here in Draining are ones already mid-flight.
     *
     * @param msg the RPC received from the upstream
     */
    public void onClientFilterChainComplete(Object msg) {
        if (state() instanceof Forwarding || state() instanceof ClientConnectionState.Draining) {
            serverConnections.values().iterator().next().sendRequest(msg);
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
        if (state() instanceof Forwarding) {
            if (!(msg instanceof RequestFrame)) {
                illegalState("Unexpected message received: " + (msg == null ? "null" : "message class=" + msg.getClass()));
                return;
            }
            if (!transportSubjectReady) {
                frontendHandler.bufferMsg(msg);
            }
            else {
                frontendHandler.admitToFilterChain(msg);
            }
        }
        else if (state() instanceof ClientConnectionState.Draining draining) {
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
     * Callback from {@link ServerConnectionStateMachine} when the upstream connection has been disconnected.
     * @param disconnectCause the cause of the disconnection
     */
    void onServerConnectionClosed(ServerConnectionStateMachine scsm,
                                  DisconnectCause disconnectCause) {
        toClosed(null, disconnectCause);
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
     * Idempotent: if the connection is already draining, no new timer is scheduled and the
     * returned future completes when the in-progress drain finishes.
     * <p>
     * Mechanics (first call):
     * <ul>
     *   <li>Dispatches onto the event loop, where a force-close timer is scheduled after
     *       {@code timeout} and {@link #onDraining(Runnable, CompletableFuture)} transitions
     *       the CCSM to {@link ClientConnectionState.Draining}, storing the returned future.</li>
     * </ul>
     * Mechanics (subsequent calls while already draining):
     * <ul>
     *   <li>Dispatches onto the event loop, finds existing {@link ClientConnectionState.Draining}
     *       state, and chains the new promise to the existing drain future — no new timer.</li>
     * </ul>
     *
     * @param timeout maximum time to wait for in-flight responses before force-closing
     * @return future that completes when this connection has reached {@link Closed}
     */
    CompletableFuture<Void> drain(Duration timeout) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        executeOnEventLoop(() -> {
            if (state instanceof ClientConnectionState.Draining existing) {
                existing.closedFuture().whenComplete((v, t) -> {
                    if (t != null) {
                        promise.completeExceptionally(t);
                    }
                    else {
                        promise.complete(null);
                    }
                });
                return;
            }
            ScheduledFuture<?> timeoutTask = scheduleOnEventLoop(this::onDrainTimeout, timeout);
            Runnable onDrained = () -> {
                timeoutTask.cancel(false);
                promise.complete(null);
                onDrainCompleted();
            };
            onDraining(onDrained, promise);
        });
        return promise;
    }

    /**
     * Begin draining: disable autoRead on the downstream channel and transition to
     * {@link ClientConnectionState.Draining}, carrying the injected {@code onDrained} policy
     * and the {@code closedFuture} that callers wait on.
     * If no requests are already in-flight, the policy fires immediately.
     * <p>
     * Internal: invoked only from {@link #drain(Duration)}, which dispatches it
     * onto the event loop. Must run on the channel's event loop thread.
     *
     * @param onDrained policy to invoke when the in-flight counter reaches zero; responsible
     *                  for closing the connection (via {@link #onDrainCompleted()}) and any
     *                  bookkeeping (future completion, timer cancellation)
     * @param closedFuture future completed by {@code onDrained}; stored in the state so that
     *                     subsequent {@link #drain(Duration)} calls can chain to it
     */
    private void onDraining(Runnable onDrained, CompletableFuture<Void> closedFuture) {
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

        setState(new ClientConnectionState.Draining(onDrained, closedFuture));
        LOGGER.atInfo()
                .addKeyValue("sessionId", kafkaSession.sessionId())
                .addKeyValue("virtualCluster", clusterName())
                .addKeyValue("clientMessagesInFlightCount", clientMessagesInFlightCount)
                .addKeyValue("serverMessagesInFlightCount",
                        serverConnections.values().stream().mapToInt(ServerConnectionStateMachine::serverMessagesInFlightCount).sum())
                .log("Connection draining started — autoRead disabled, waiting for in-flight responses");

        if (clientMessagesInFlightCount <= 0) {
            LOGGER.atInfo()
                    .addKeyValue("sessionId", kafkaSession.sessionId())
                    .addKeyValue("virtualCluster", clusterName())
                    .log("No in-flight requests — signalling drain policy immediately");
            onDrained.run();
        }
    }

    /**
     * Drain completed naturally: invoked by the {@code onDrained} policy when the in-flight
     * counter reaches zero, transitioning the connection to {@link Closed} with the
     * {@code DRAIN_COMPLETED} cause for metrics. No-op if the state has already transitioned
     * away from {@link ClientConnectionState.Draining}.
     * <p>
     * Internal: only called from the policy assembled in {@link #drain(Duration)}.
     */
    private void onDrainCompleted() {
        if (state instanceof ClientConnectionState.Draining) {
            toClosed(null, DisconnectCause.DRAIN_COMPLETED);
        }
    }

    /**
     * The drain timeout timer expired. Force-closes the connection with the {@code DRAIN_TIMEOUT}
     * cause for metrics. No-op if the state has already transitioned away from
     * {@link ClientConnectionState.Draining} (e.g. drain already completed or the connection closed
     * for another reason).
     * <p>
     * Internal: only invoked by the timer scheduled in {@link #drain(Duration)}.
     */
    private void onDrainTimeout() {
        if (state instanceof ClientConnectionState.Draining) {
            LOGGER.atWarn()
                    .addKeyValue("sessionId", kafkaSession.sessionId())
                    .addKeyValue("virtualCluster", clusterName())
                    .addKeyValue("clientMessagesInFlightCount", clientMessagesInFlightCount)
                    .log("Drain timeout expired — force-closing connection");
            toClosed(null, DisconnectCause.DRAIN_TIMEOUT);
        }
    }

    /**
     * Callback from {@link ServerConnectionStateMachine} when something exceptional and
     * un-recoverable has happened on the upstream side.
     * @param cause the exception that triggered the issue
     */
    void onServerConnectionException(ServerConnectionStateMachine scsm,
                                     @Nullable Throwable cause) {
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
            log(Level.WARN)
                    .addKeyValue("maxFrameSizeBytes", e.getMaxFrameSizeBytes())
                    .addKeyValue("receivedFrameSizeBytes", e.getReceivedFrameSizeBytes())
                    .addKeyValue("hint", tlsHint)
                    .log("Received over-sized frame from client, other possible causes are: an oversized Kafka frame, or something unexpected like an HTTP request");
            errorCodeEx = Errors.INVALID_REQUEST.exception();
        }
        else {
            log(Level.WARN)
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
                                ClientConnectionState.ClientActive clientActive,
                                KafkaProxyFrontendHandler frontendHandler) {
        setState(clientActive);
        this.transportSubjectReady = false;
        if (!this.isTlsListener()) {
            this.clientSubjectManager.subjectFromTransport(null, this.transportSubjectBuilder,
                    frontendHandler.eventLoopExecutor(), this::onTransportSubjectBuilt);
        }
        frontendHandler.inClientActive();

        clientToProxyConnectionCounter.increment();
        clientToProxyConnectionToken.acquire();

    }

    void onTransportSubjectBuilt() {
        if (!authenticatedSubject().isAnonymous()) {
            onSessionTransportAuthenticated();
        }
        this.transportSubjectReady = true;
        tryUnblockClient();
    }

    public Subject authenticatedSubject() {
        return Objects.requireNonNull(clientSubjectManager).authenticatedSubject();
    }

    private void tryUnblockClient() {
        if (transportSubjectReady && state instanceof Forwarding) {
            Objects.requireNonNull(frontendHandler).unblockClient();
        }
    }

    @SuppressWarnings("java:S5738")
    private void toForwarding(Forwarding forwarding,
                              HostPort remote) {
        setState(forwarding);
        proxyToServerConnectionCounter.increment();
        var scsm = createServerConnection(remote);
        serverConnections.put(remote, scsm);
        var frontend = Objects.requireNonNull(frontendHandler);
        scsm.connect(Objects.requireNonNull(frontend.clientChannel()));
        log(Level.DEBUG)
                .addKeyValue("remote", remote)
                .addKeyValue("clientAddress", () -> HostPort.asString(frontend.remoteHost(), frontend.remotePort()))
                .log("Upstream connection initiated for client");
    }

    @FunctionalInterface
    interface ServerConnectionFactory {
        @SuppressWarnings("java:S107")
        ServerConnectionStateMachine create(HostPort remote,
                                            ClientConnectionStateMachine ccsm,
                                            VirtualClusterModel virtualCluster,
                                            String clusterName,
                                            @Nullable Integer nodeId,
                                            Counter proxyToServerErrorCounter,
                                            Timer serverToProxyBackpressureMeter,
                                            ActivationToken proxyToServerConnectionToken);
    }

    @SuppressWarnings("java:S5738")
    private void toForwardingWithRoutes(Forwarding forwarding) {
        setState(forwarding);
        Map<String, RouteDescriptor> descriptors = virtualCluster().routeDescriptors();
        routeTargets = new HashMap<>();
        var frontend = Objects.requireNonNull(frontendHandler);
        Channel clientChannel = Objects.requireNonNull(frontend.clientChannel());
        for (var entry : descriptors.entrySet()) {
            RouteDescriptor rd = entry.getValue();
            if (rd.targetsCluster()) {
                HostPort target = rd.targetCluster().bootstrapServer();
                routeTargets.put(entry.getKey(), target);
                serverConnections.computeIfAbsent(target, t -> {
                    proxyToServerConnectionCounter.increment();
                    var newScsm = createServerConnection(t);
                    newScsm.connect(clientChannel);
                    return newScsm;
                });
            }
        }
        log(Level.DEBUG)
                .addKeyValue("routeCount", routeTargets.size())
                .addKeyValue("backendCount", serverConnections.size())
                .log("Upstream connections initiated for routing VC");
    }

    /**
     * Forward a message to the backend connection for the named route.
     * Used by {@link io.kroxylicious.proxy.internal.routing.RouterDispatchHandler}
     * for both static and dynamic routing paths.
     */
    public void forwardToRoute(String routeName, Object msg) {
        if (state() instanceof Forwarding || state() instanceof ClientConnectionState.Draining) {
            HostPort target = routeTargets.get(routeName);
            if (target == null) {
                illegalState("Unknown route: " + routeName);
                return;
            }
            ServerConnectionStateMachine scsm = serverConnections.get(target);
            scsm.sendRequest(msg);
        }
        else {
            illegalState("forwardToRoute in unexpected state");
        }
    }

    @VisibleForTesting
    ServerConnectionStateMachine createServerConnection(HostPort remote) {
        return serverConnectionFactory.create(remote, this, virtualCluster(), clusterName(), nodeId(),
                proxyToServerErrorCounter, serverToProxyBackpressureMeter, proxyToServerConnectionToken);
    }

    /**
     * Handle a message received from the client prior to connecting to the upstream node.
     * Captures client software metadata from ApiVersions requests and triggers
     * the transition to {@link Forwarding}.
     * @param msg Message received from the downstream client.
     * @return {@code false} for unsupported message types
     */
    private boolean onClientRequestBeforeForwarding(Object msg) {
        Objects.requireNonNull(frontendHandler).bufferMsg(msg);
        if (state() instanceof ClientConnectionState.ClientActive clientActive) {
            return transitionToForwarding(msg, clientActive::toForwarding);
        }
        else if (state() instanceof ClientConnectionState.HaProxy haProxy) {
            return transitionToForwarding(msg, haProxy::toForwarding);
        }
        return false;
    }

    private boolean transitionToForwarding(
                                           Object msg,
                                           Supplier<Forwarding> forwardingFactory) {
        if (isMessageApiVersionsRequest(msg)) {
            @SuppressWarnings("unchecked")
            DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame = (DecodedRequestFrame<ApiVersionsRequestData>) msg;
            this.clientSoftwareName = apiVersionsFrame.body().clientSoftwareName();
            this.clientSoftwareVersion = apiVersionsFrame.body().clientSoftwareVersion();
        }
        if (msg instanceof RequestFrame) {
            if (virtualCluster().usesRouter()) {
                toForwardingWithRoutes(forwardingFactory.get());
            }
            else {
                var target = Objects.requireNonNull(endpointBinding.upstreamTarget());
                toForwarding(forwardingFactory.get(), target);
            }
            tryUnblockClient();
            return true;
        }
        return false;
    }

    private void toHaProxy(ClientConnectionState.HaProxy haProxy) {
        setState(haProxy);
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
        // toClosed from inside the callback itself (messageFromServer → onDrained → ccsm.onDrainCompleted
        // → toClosed) — re-firing here would invoke the callback twice. For any other cause
        // (DRAIN_TIMEOUT, orphan server/client errors during drain) the callback has NOT run yet
        // and must fire so the coordinator's future unblocks and its timer cancels.
        Runnable pendingDrainCallback = (state instanceof ClientConnectionState.Draining draining
                && disconnectCause != DisconnectCause.DRAIN_COMPLETED) ? draining.onDrained() : null;

        setState(new Closed());

        incrementAppropriateDisconnectsMetric(disconnectCause);

        kafkaSession.transitionTo(KafkaSessionState.TERMINATING);
        // Close all server connections
        for (ServerConnectionStateMachine scsm : serverConnections.values()) {
            scsm.close();
        }
        serverConnections.clear();

        // Close the client connection
        if (frontendHandler != null) { // Can be null if the error happens before clientActive (unlikely but possible)
            frontendHandler.inClosed(errorCodeEx);
            clientToProxyConnectionToken.release();
        }

        // Fire the drain policy if we were draining when we entered toClosed — signals the
        // coordinator that this connection is closed regardless of whether drain completed
        // naturally or the connection was torn down for another reason.
        if (pendingDrainCallback != null) {
            LOGGER.atInfo()
                    .addKeyValue("sessionId", kafkaSession.sessionId())
                    .addKeyValue("virtualCluster", clusterName())
                    .addKeyValue("disconnectCause", disconnectCause)
                    .addKeyValue("errorCodeEx", errorCodeEx == null ? null : errorCodeEx.getClass().getSimpleName() + ": " + errorCodeEx.getMessage())
                    .addKeyValue("clientMessagesInFlightCount", clientMessagesInFlightCount)
                    .addKeyValue("serverMessagesInFlightCount",
                            serverConnections.values().stream().mapToInt(ServerConnectionStateMachine::serverMessagesInFlightCount).sum())
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

    private void setState(ClientConnectionState state) {
        log(Level.TRACE)
                .addKeyValue("stateMachine", this)
                .addKeyValue("targetState", state)
                .log("Transitioning to state");
        this.state = state;
    }

    private LoggingEventBuilder log(Level level) {
        LoggingEventBuilder builder = switch (level) {
            case ERROR -> LOGGER.atError();
            case WARN -> LOGGER.atWarn();
            case INFO -> LOGGER.atInfo();
            case DEBUG -> LOGGER.atDebug();
            case TRACE -> LOGGER.atTrace();
        };
        return builder
                .addKeyValue("sessionId", kafkaSession.sessionId())
                .addKeyValue("virtualCluster", clusterName());
    }

    private static boolean isMessageApiVersionsRequest(Object msg) {
        return msg instanceof DecodedRequestFrame
                && ((DecodedRequestFrame<?>) msg).apiKey() == ApiKeys.API_VERSIONS;
    }

    /**
     * Dispatch a task onto this CCSM's event loop. Used internally so that {@code on*()}
     * transition methods always run on the channel's event loop thread, regardless of which
     * thread {@link #drain(Duration)} is invoked from.
     * @throws IllegalStateException if the CCSM has no frontend channel attached yet
     */
    private void executeOnEventLoop(Runnable task) {
        requireFrontendChannel().eventLoop().execute(task);
    }

    /**
     * Schedule a delayed task onto this CCSM's event loop. Returns the scheduled-future handle
     * so callers can cancel it. Used internally to schedule the drain force-close timer.
     * @throws IllegalStateException if the CCSM has no frontend channel attached yet
     */
    private ScheduledFuture<?> scheduleOnEventLoop(Runnable task, Duration delay) {
        return requireFrontendChannel().eventLoop().schedule(task, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private Channel requireFrontendChannel() {
        Channel ch = frontendHandler != null ? frontendHandler.clientChannel() : null;
        if (ch == null) {
            throw new IllegalStateException("CCSM has no frontend channel attached — dispatch not possible in state " + state.getClass().getSimpleName());
        }
        return ch;
    }

}
