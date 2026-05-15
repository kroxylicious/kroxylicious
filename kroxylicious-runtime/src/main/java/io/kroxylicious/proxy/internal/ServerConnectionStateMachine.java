/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.event.Level;
import org.slf4j.spi.LoggingEventBuilder;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

import io.kroxylicious.proxy.internal.util.ActivationToken;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Manages the lifecycle of a single upstream (proxy-to-broker) TCP connection.
 * <p>
 * Extracted from {@link ProxyChannelStateMachine} to separate server-side connection
 * concerns from the client session. The PCSM retains client-side state and delegates
 * server operations here.
 *
 * <pre>
 *     Connecting ──→ Active ──→ [Draining] ──→ Closed
 *         │             │            │
 *         └─────────────┴────────────┴──→ Closed (on error)
 * </pre>
 */
class ServerConnectionStateMachine {

    private static final Logger LOGGER = getLogger(ServerConnectionStateMachine.class);

    private ServerConnectionState state;

    private final ProxyChannelStateMachine pcsm;
    private final KafkaProxyBackendHandler backendHandler;
    private final boolean upstreamRequiresTls;

    @VisibleForTesting
    int serverMessagesInFlightCount;

    @VisibleForTesting
    boolean serverReadsBlocked;

    @VisibleForTesting
    @Nullable
    Timer.Sample serverBackpressureTimer;

    private final Counter proxyToServerConnectionCounter;
    private final Counter proxyToServerErrorCounter;
    private final Timer serverToProxyBackpressureMeter;
    private final ActivationToken proxyToServerConnectionToken;

    ServerConnectionStateMachine(
                                 HostPort remote,
                                 boolean upstreamRequiresTls,
                                 ProxyChannelStateMachine pcsm,
                                 Counter proxyToServerConnectionCounter,
                                 Counter proxyToServerErrorCounter,
                                 Timer serverToProxyBackpressureMeter,
                                 ActivationToken proxyToServerConnectionToken) {
        this.state = new ServerConnectionState.Connecting(remote);
        this.upstreamRequiresTls = upstreamRequiresTls;
        this.pcsm = Objects.requireNonNull(pcsm);
        this.backendHandler = new KafkaProxyBackendHandler(this);
        this.proxyToServerConnectionCounter = proxyToServerConnectionCounter;
        this.proxyToServerErrorCounter = proxyToServerErrorCounter;
        this.serverToProxyBackpressureMeter = serverToProxyBackpressureMeter;
        this.proxyToServerConnectionToken = proxyToServerConnectionToken;
        proxyToServerConnectionCounter.increment();
    }

    ServerConnectionState state() {
        return state;
    }

    KafkaProxyBackendHandler backendHandler() {
        return backendHandler;
    }

    boolean isUpstreamTls() {
        return upstreamRequiresTls;
    }

    // === Events from KafkaProxyBackendHandler ===

    void onServerActive() {
        if (state instanceof ServerConnectionState.Connecting connecting) {
            setState(connecting.toActive());
            proxyToServerConnectionToken.acquire();
            pcsm.onServerConnectionActive();
        }
        else {
            pcsm.illegalState("Server became active while not in the connecting state");
        }
    }

    void onServerInactive() {
        if (!(state instanceof ServerConnectionState.Closed)) {
            toClosed();
            pcsm.onServerConnectionClosed(ProxyChannelStateMachine.DisconnectCause.SERVER_CLOSED);
        }
    }

    @SuppressWarnings("java:S5738")
    void onServerException(@Nullable Throwable cause) {
        if (!(state instanceof ServerConnectionState.Closed)) {
            log(Level.WARN)
                    .addKeyValue("error", cause != null ? cause.getMessage() : "")
                    .setCause(LOGGER.isDebugEnabled() ? cause : null)
                    .log(LOGGER.isDebugEnabled()
                            ? "exception from server channel"
                            : "exception from server channel, increase log level to DEBUG for stacktrace");
            proxyToServerErrorCounter.increment();
            toClosed();
            pcsm.onServerConnectionException(cause);
        }
    }

    void onMessageFromServer(Object msg) {
        serverMessagesInFlightCount = Math.max(0, serverMessagesInFlightCount - 1);
        pcsm.onResponseFromServer(msg);
    }

    void serverReadComplete() {
        pcsm.onServerReadComplete();
    }

    void onServerUnwritable() {
        pcsm.onServerUnwritable();
    }

    void onServerWritable() {
        pcsm.onServerWritable();
    }

    // === Called by ProxyChannelStateMachine ===

    void sendRequest(Object msg) {
        serverMessagesInFlightCount++;
        backendHandler.forwardToServer(msg);
        backendHandler.flushToServer();
    }

    void applyBackpressure() {
        if (!serverReadsBlocked) {
            serverReadsBlocked = true;
            serverBackpressureTimer = Timer.start();
            backendHandler.applyBackpressure();
        }
    }

    void relieveBackpressure() {
        if (serverReadsBlocked) {
            serverReadsBlocked = false;
            if (serverBackpressureTimer != null) {
                serverBackpressureTimer.stop(serverToProxyBackpressureMeter);
                serverBackpressureTimer = null;
            }
            backendHandler.relieveBackpressure();
        }
    }

    void close() {
        if (!(state instanceof ServerConnectionState.Closed)) {
            toClosed();
        }
    }

    private void toClosed() {
        setState(new ServerConnectionState.Closed());
        backendHandler.inClosed();
        proxyToServerConnectionToken.release();
    }

    private void setState(ServerConnectionState newState) {
        log(Level.TRACE)
                .addKeyValue("targetState", newState)
                .log("Server connection transitioning to state");
        this.state = newState;
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
                .addKeyValue("sessionId", pcsm.sessionId())
                .addKeyValue("virtualCluster", pcsm.clusterName());
    }

    @Override
    public String toString() {
        return "ServerConnectionStateMachine{" +
                "state=" + state +
                ", serverReadsBlocked=" + serverReadsBlocked +
                ", serverMessagesInFlightCount=" + serverMessagesInFlightCount +
                '}';
    }
}
