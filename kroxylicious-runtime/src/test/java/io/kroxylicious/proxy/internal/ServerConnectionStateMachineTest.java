/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import io.kroxylicious.proxy.internal.util.ActivationToken;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ServerConnectionStateMachineTest {

    private static final HostPort BROKER_ADDRESS = new HostPort("broker.example.com", 9092);
    private static final Offset<Double> CLOSE_ENOUGH = Offset.offset(0.00005);

    @Mock
    private ClientConnectionStateMachine ccsm;

    @Mock
    private ActivationToken activationToken;

    private ServerConnectionStateMachine serverStateMachine;
    private SimpleMeterRegistry meterRegistry;
    private Counter proxyToServerConnectionCounter;
    private Counter proxyToServerErrorCounter;
    private Timer serverToProxyBackpressureMeter;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);

        proxyToServerConnectionCounter = meterRegistry.counter("test.proxy.to.server.connections");
        proxyToServerErrorCounter = meterRegistry.counter("test.proxy.to.server.errors");
        serverToProxyBackpressureMeter = meterRegistry.timer("test.server.to.proxy.backpressure");

        lenient().when(ccsm.sessionId()).thenReturn("test-session-123");
        lenient().when(ccsm.clusterName()).thenReturn("test-cluster");

        serverStateMachine = new ServerConnectionStateMachine(
                BROKER_ADDRESS,
                false,
                ccsm,
                proxyToServerConnectionCounter,
                proxyToServerErrorCounter,
                serverToProxyBackpressureMeter,
                activationToken);
    }

    @AfterEach
    void tearDown() {
        if (meterRegistry != null) {
            meterRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
            Metrics.globalRegistry.remove(meterRegistry);
        }
    }

    @Test
    void shouldStartInConnectingState() {
        assertThat(serverStateMachine.state())
                .isInstanceOf(ServerConnectionState.Connecting.class)
                .extracting(state -> ((ServerConnectionState.Connecting) state).remote())
                .isEqualTo(BROKER_ADDRESS);
    }

    @Test
    void shouldIncrementConnectionCounterOnConstruction() {
        assertThat(proxyToServerConnectionCounter.count())
                .isCloseTo(1.0, CLOSE_ENOUGH);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldTrackTlsRequirement(boolean requiresTls) {
        ServerConnectionStateMachine sm = new ServerConnectionStateMachine(
                BROKER_ADDRESS,
                requiresTls,
                ccsm,
                proxyToServerConnectionCounter,
                proxyToServerErrorCounter,
                serverToProxyBackpressureMeter,
                activationToken);

        assertThat(sm.isUpstreamTls()).isEqualTo(requiresTls);
    }

    @Test
    void shouldReturnBackendHandler() {
        assertThat(serverStateMachine.backendHandler()).isNotNull();
    }

    @Test
    void onServerActiveShouldTransitionToActiveAndNotifyClientStateMachine() {
        serverStateMachine.onServerActive();

        assertThat(serverStateMachine.state()).isInstanceOf(ServerConnectionState.Active.class);
        verify(activationToken).acquire();
        verify(ccsm).onServerConnectionActive();
    }

    @Test
    void onServerActiveWhenNotInConnectingStateShouldReportIllegalState() {
        serverStateMachine.onServerActive();
        assertThat(serverStateMachine.state()).isInstanceOf(ServerConnectionState.Active.class);

        serverStateMachine.onServerActive();

        verify(ccsm).illegalState("Server became active while not in the connecting state");
    }

    @Test
    void onServerInactiveShouldTransitionToClosedAndNotifyClientStateMachine() {
        serverStateMachine.onServerActive();

        serverStateMachine.onServerInactive();

        assertThat(serverStateMachine.state()).isInstanceOf(ServerConnectionState.Closed.class);
        verify(ccsm).onServerConnectionClosed(ClientConnectionStateMachine.DisconnectCause.SERVER_CLOSED);
        verify(activationToken).release();
    }

    @Test
    void onServerInactiveWhenAlreadyClosedShouldNotNotifyAgain() {
        serverStateMachine.close();

        serverStateMachine.onServerInactive();

        verify(ccsm, never()).onServerConnectionClosed(ClientConnectionStateMachine.DisconnectCause.SERVER_CLOSED);
    }

    @Test
    void onServerExceptionShouldIncrementErrorCounterAndTransitionToClosed() {
        serverStateMachine.onServerActive();
        RuntimeException cause = new RuntimeException("Connection failed");

        serverStateMachine.onServerException(cause);

        assertThat(serverStateMachine.state()).isInstanceOf(ServerConnectionState.Closed.class);
        assertThat(proxyToServerErrorCounter.count()).isCloseTo(1.0, CLOSE_ENOUGH);
        verify(ccsm).onServerConnectionException(cause);
        verify(activationToken).release();
    }

    @Test
    void onServerExceptionWithNullCauseShouldStillTransitionToClosed() {
        serverStateMachine.onServerActive();

        serverStateMachine.onServerException(null);

        assertThat(serverStateMachine.state()).isInstanceOf(ServerConnectionState.Closed.class);
        assertThat(proxyToServerErrorCounter.count()).isCloseTo(1.0, CLOSE_ENOUGH);
        verify(ccsm).onServerConnectionException(null);
    }

    @Test
    void onServerExceptionWhenAlreadyClosedShouldNotIncrementErrorCounter() {
        serverStateMachine.close();
        double errorCountBefore = proxyToServerErrorCounter.count();

        serverStateMachine.onServerException(new RuntimeException("test"));

        assertThat(proxyToServerErrorCounter.count()).isEqualTo(errorCountBefore);
        verify(ccsm, never()).onServerConnectionException(null);
    }

    @Test
    void onMessageFromServerShouldDecrementInFlightCountAndDelegateToCcsm() {
        serverStateMachine.serverMessagesInFlightCount = 2;
        Object message = new Object();

        serverStateMachine.onMessageFromServer(message);

        assertThat(serverStateMachine.serverMessagesInFlightCount).isEqualTo(1);
        verify(ccsm).onResponseFromServer(message);
    }

    @Test
    void onMessageFromServerShouldNotDecrementBelowZero() {
        serverStateMachine.serverMessagesInFlightCount = 0;
        Object message = new Object();

        serverStateMachine.onMessageFromServer(message);

        assertThat(serverStateMachine.serverMessagesInFlightCount).isZero();
        verify(ccsm).onResponseFromServer(message);
    }

    @Test
    void serverReadCompleteShouldDelegateToCcsm() {
        serverStateMachine.serverReadComplete();

        verify(ccsm).onServerReadComplete();
    }

    @Test
    void onServerUnwritableShouldDelegateToCcsm() {
        serverStateMachine.onServerUnwritable();

        verify(ccsm).onServerUnwritable();
    }

    @Test
    void onServerWritableShouldDelegateToCcsm() {
        serverStateMachine.onServerWritable();

        verify(ccsm).onServerWritable();
    }

    @Test
    void sendRequestShouldIncrementInFlightCount() {
        serverStateMachine.onServerActive();
        Object message = new Object();

        serverStateMachine.sendRequest(message);

        assertThat(serverStateMachine.serverMessagesInFlightCount).isEqualTo(1);
    }

    @Test
    void sendRequestShouldIncrementInFlightCountMultipleTimes() {
        serverStateMachine.onServerActive();

        serverStateMachine.sendRequest(new Object());
        serverStateMachine.sendRequest(new Object());
        serverStateMachine.sendRequest(new Object());

        assertThat(serverStateMachine.serverMessagesInFlightCount).isEqualTo(3);
    }

    @Test
    void applyBackpressureShouldStartTimer() {
        serverStateMachine.onServerActive();

        serverStateMachine.applyBackpressure();

        assertThat(serverStateMachine).extracting("serverBackpressureTimer").isNotNull();
    }

    @Test
    void applyBackpressureShouldBeIdempotent() {
        serverStateMachine.onServerActive();

        serverStateMachine.applyBackpressure();
        Object firstTimer = serverStateMachine.serverBackpressureTimer;
        serverStateMachine.applyBackpressure();
        Object secondTimer = serverStateMachine.serverBackpressureTimer;

        assertThat(firstTimer).isSameAs(secondTimer);
    }

    @Test
    void relieveBackpressureShouldStopTimerAndRecordMetric() {
        serverStateMachine.onServerActive();

        serverStateMachine.applyBackpressure();
        serverStateMachine.relieveBackpressure();

        assertThat(serverStateMachine).extracting("serverBackpressureTimer").isNull();
        assertThat(serverToProxyBackpressureMeter.count()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void relieveBackpressureShouldBeIdempotent() {
        serverStateMachine.onServerActive();

        serverStateMachine.applyBackpressure();
        serverStateMachine.relieveBackpressure();
        long countAfterFirst = serverToProxyBackpressureMeter.count();
        serverStateMachine.relieveBackpressure();
        long countAfterSecond = serverToProxyBackpressureMeter.count();

        assertThat(countAfterSecond).isEqualTo(countAfterFirst);
    }

    @Test
    void relieveBackpressureWhenNotAppliedShouldBeNoOp() {
        serverStateMachine.relieveBackpressure();

        assertThat(serverStateMachine).extracting("serverBackpressureTimer").isNull();
        assertThat(serverToProxyBackpressureMeter.count()).isZero();
    }

    @Test
    void closeShouldTransitionToClosedAndReleaseActivationToken() {
        serverStateMachine.onServerActive();

        serverStateMachine.close();

        assertThat(serverStateMachine.state()).isInstanceOf(ServerConnectionState.Closed.class);
        verify(activationToken).release();
    }

    @Test
    void closeShouldBeIdempotent() {
        serverStateMachine.onServerActive();

        serverStateMachine.close();
        serverStateMachine.close();

        verify(activationToken, times(1)).release();
    }

    @Test
    void toStringShouldIncludeStateAndCounters() {
        serverStateMachine.serverMessagesInFlightCount = 3;

        String result = serverStateMachine.toString();

        assertThat(result)
                .contains("ServerConnectionStateMachine")
                .contains("state=")
                .contains("serverReadsBlocked=")
                .contains("serverMessagesInFlightCount=3");
    }

    @Test
    void shouldReleaseActivationTokenOnlyOnceWhenTransitioningToClosed() {
        serverStateMachine.onServerActive();

        serverStateMachine.close();
        serverStateMachine.onServerInactive();
        serverStateMachine.onServerException(new RuntimeException("test"));

        verify(activationToken, times(1)).release();
    }

    @Test
    void shouldNotifyClientStateMachineOnEachEventBeforeClosing() {
        serverStateMachine.onServerActive();
        RuntimeException exception = new RuntimeException("test exception");

        serverStateMachine.onServerException(exception);

        verify(ccsm).onServerConnectionException(exception);
    }
}
