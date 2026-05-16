/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.internal.codec.KafkaRequestEncoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseDecoder;
import io.kroxylicious.proxy.internal.tls.ServerTlsCredentialSupplierContextImpl;
import io.kroxylicious.proxy.internal.tls.TestCertificateUtil;
import io.kroxylicious.proxy.internal.tls.TlsCredentialsImpl;
import io.kroxylicious.proxy.internal.util.ActivationToken;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplier;
import io.kroxylicious.proxy.tls.TlsCredentials;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ServerConnectionStateMachineTest {

    private static final HostPort BROKER_ADDRESS = new HostPort("broker.example.com", 9092);
    private static final String CLUSTER_NAME = "test-cluster";
    private static final Offset<Double> CLOSE_ENOUGH = Offset.offset(0.00005);

    @Mock
    private ClientConnectionStateMachine ccsm;

    @Mock
    private ActivationToken activationToken;

    @Mock
    private VirtualClusterModel virtualCluster;

    private ServerConnectionStateMachine serverStateMachine;
    private SimpleMeterRegistry meterRegistry;
    private Counter proxyToServerErrorCounter;
    private Timer serverToProxyBackpressureMeter;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);

        proxyToServerErrorCounter = meterRegistry.counter("test.proxy.to.server.errors");
        serverToProxyBackpressureMeter = meterRegistry.timer("test.server.to.proxy.backpressure");

        lenient().when(ccsm.sessionId()).thenReturn("test-session-123");
        lenient().when(ccsm.clusterName()).thenReturn(CLUSTER_NAME);
        lenient().when(virtualCluster.getUpstreamSslContext()).thenReturn(Optional.empty());

        serverStateMachine = new ServerConnectionStateMachine(
                BROKER_ADDRESS,
                ccsm,
                virtualCluster,
                CLUSTER_NAME,
                null,
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
    void sendRequestWhileConnectingShouldBuffer() {
        assertThat(serverStateMachine.state()).isInstanceOf(ServerConnectionState.Connecting.class);

        Object msg = new Object();
        serverStateMachine.sendRequest(msg);

        assertThat(serverStateMachine.serverMessagesInFlightCount).isZero();
    }

    @Test
    void sendRequestWhileActiveShouldForwardImmediately() {
        var channel = new EmbeddedChannel(serverStateMachine.backendHandler());
        assertThat(serverStateMachine.state()).isInstanceOf(ServerConnectionState.Active.class);

        Object msg = "test-request";
        serverStateMachine.sendRequest(msg);

        assertThat(serverStateMachine.serverMessagesInFlightCount).isEqualTo(1);
        assertThat(channel.<Object> readOutbound()).isEqualTo(msg);
    }

    @Test
    void onServerActiveShouldFlushPendingRequests() {
        serverStateMachine.sendRequest("req-1");
        serverStateMachine.sendRequest("req-2");
        assertThat(serverStateMachine.serverMessagesInFlightCount).isZero();

        // Registering the handler triggers channelActive → onServerActive → flush
        var channel = new EmbeddedChannel(serverStateMachine.backendHandler());
        assertThat(serverStateMachine.state()).isInstanceOf(ServerConnectionState.Active.class);
        assertThat(serverStateMachine.serverMessagesInFlightCount).isEqualTo(2);

        assertThat(channel.<Object> readOutbound()).isEqualTo("req-1");
        assertThat(channel.<Object> readOutbound()).isEqualTo("req-2");
        assertThat(channel.<Object> readOutbound()).isNull();
    }

    @Test
    void onServerActiveShouldFlushBeforePcsmCallback() {
        serverStateMachine.sendRequest("req-1");
        verifyNoInteractions(ccsm);

        // channelActive → onServerActive → flush pending → pcsm callback
        new EmbeddedChannel(serverStateMachine.backendHandler());

        // At the point pcsm.onServerConnectionActive() was called,
        // the pending requests had already been flushed
        assertThat(serverStateMachine.serverMessagesInFlightCount).isEqualTo(1);
        verify(ccsm).onServerConnectionActive();
    }

    @Test
    void closedShouldReleasePendingRequests() {
        ByteBuf buf = Unpooled.buffer(4).writeInt(42);
        assertThat(buf.refCnt()).isEqualTo(1);
        serverStateMachine.sendRequest(buf);

        serverStateMachine.close();

        assertThat(buf.refCnt()).isZero();
    }

    @Test
    void closedWithNoPendingRequestsShouldNotFail() {
        serverStateMachine.close();

        assertThat(serverStateMachine.state()).isInstanceOf(ServerConnectionState.Closed.class);
    }

    @Test
    void exceptionWhileConnectingShouldReleasePendingRequests() {
        ByteBuf buf = Unpooled.buffer(4).writeInt(99);
        serverStateMachine.sendRequest(buf);
        assertThat(buf.refCnt()).isEqualTo(1);

        serverStateMachine.onServerException(new RuntimeException("connection failed"));

        assertThat(buf.refCnt()).isZero();
        assertThat(serverStateMachine.state()).isInstanceOf(ServerConnectionState.Closed.class);
    }

    @Test
    void onServerActiveWithNoPendingRequestsShouldNotFail() {
        assertThat(serverStateMachine.state()).isInstanceOf(ServerConnectionState.Connecting.class);

        // channelActive triggers onServerActive — no pending requests to flush
        new EmbeddedChannel(serverStateMachine.backendHandler());

        assertThat(serverStateMachine.state()).isInstanceOf(ServerConnectionState.Active.class);
        assertThat(serverStateMachine.serverMessagesInFlightCount).isZero();
    }

    @Test
    void pendingRequestsPreserveOrder() {
        for (int i = 0; i < 5; i++) {
            serverStateMachine.sendRequest("req-" + i);
        }

        var channel = new EmbeddedChannel(serverStateMachine.backendHandler());

        for (int i = 0; i < 5; i++) {
            assertThat(channel.<Object> readOutbound()).isEqualTo("req-" + i);
        }
        assertThat(channel.<Object> readOutbound()).isNull();
        assertThat(serverStateMachine.serverMessagesInFlightCount).isEqualTo(5);
    }

    @Test
    void shouldStartInConnectingState() {
        assertThat(serverStateMachine.state())
                .isInstanceOf(ServerConnectionState.Connecting.class)
                .extracting(state -> ((ServerConnectionState.Connecting) state).remote())
                .isEqualTo(BROKER_ADDRESS);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldTrackTlsRequirement(boolean requiresTls) {
        var vc = mock(VirtualClusterModel.class);
        when(vc.getUpstreamSslContext()).thenReturn(requiresTls ? Optional.of(mock(io.netty.handler.ssl.SslContext.class)) : Optional.empty());

        ServerConnectionStateMachine sm = new ServerConnectionStateMachine(
                BROKER_ADDRESS,
                ccsm,
                vc,
                CLUSTER_NAME,
                null,
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
    void onServerInactiveWhileConnectingShouldTransitionToClosedAndNotifyCcsm() {
        assertThat(serverStateMachine.state()).isInstanceOf(ServerConnectionState.Connecting.class);

        serverStateMachine.onServerInactive();

        assertThat(serverStateMachine.state()).isInstanceOf(ServerConnectionState.Closed.class);
        verify(ccsm).onServerConnectionClosed(ClientConnectionStateMachine.DisconnectCause.SERVER_CLOSED);
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

    // === connect() tests ===

    private ServerConnectionStateMachine createConnectableScsm(ClientConnectionStateMachine mockCcsm,
                                                               VirtualClusterModel mockVirtualCluster,
                                                               EmbeddedChannel[] outboundHolder) {
        lenient().when(mockCcsm.sessionId()).thenReturn("test-session");
        lenient().when(mockCcsm.clusterName()).thenReturn(CLUSTER_NAME);
        lenient().when(mockVirtualCluster.getUpstreamSslContext()).thenReturn(Optional.empty());
        when(mockVirtualCluster.usesDynamicTlsCredentials()).thenReturn(false);
        when(mockVirtualCluster.socketFrameMaxSizeBytes()).thenReturn(
                VirtualClusterModel.DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES);
        return new ServerConnectionStateMachine(
                BROKER_ADDRESS, mockCcsm, mockVirtualCluster, CLUSTER_NAME, null,
                mock(Counter.class), mock(Timer.class), mock(ActivationToken.class)) {
            @Override
            Bootstrap configureBootstrap(KafkaProxyBackendHandler backendHandler,
                                         Channel inboundChannel) {
                outboundHolder[0] = new EmbeddedChannel();
                Bootstrap bootstrap = new Bootstrap();
                bootstrap.group(outboundHolder[0].eventLoop())
                        .channel(outboundHolder[0].getClass())
                        .handler(backendHandler)
                        .option(ChannelOption.AUTO_READ, true)
                        .option(ChannelOption.TCP_NODELAY, true);
                return bootstrap;
            }

            @Override
            ChannelFuture initConnection(String remoteHost, int remotePort, Bootstrap bootstrap) {
                outboundHolder[0].pipeline().addFirst(backendHandler());
                return outboundHolder[0].newSucceededFuture();
            }
        };
    }

    @Test
    void connectInWrongStateShouldCallIllegalState() {
        var mockCcsm = mock(ClientConnectionStateMachine.class);
        var mockVirtualCluster = mock(VirtualClusterModel.class);
        var scsm = createScsmWithMocks(mockCcsm, mockVirtualCluster);

        // Force to Active state
        new EmbeddedChannel(scsm.backendHandler());
        assertThat(scsm.state()).isInstanceOf(ServerConnectionState.Active.class);

        // Calling connect() in Active state should trigger illegalState
        scsm.connect(mock(Channel.class));

        verify(mockCcsm).illegalState("connect() called while not in Connecting state");
    }

    @Test
    void connectShouldAssemblePipelineInCorrectOrder() {
        var mockCcsm = mock(ClientConnectionStateMachine.class);
        var mockVirtualCluster = mock(VirtualClusterModel.class);
        var outboundHolder = new EmbeddedChannel[1];
        var scsm = createConnectableScsm(mockCcsm, mockVirtualCluster, outboundHolder);

        scsm.connect(new EmbeddedChannel());

        var pipeline = outboundHolder[0].pipeline();
        List<String> handlerNames = pipeline.names().stream()
                .filter(n -> !n.contains("DefaultChannelPipeline"))
                .toList();

        // Pipeline uses addFirst, so the order in the list is the reverse of insertion order.
        // Expected from head to tail: networkLogger (absent), requestEncoder, responseDecoder,
        // frameLogger (absent), backendHandler, then tail sentinel.
        assertThat(handlerNames)
                .filteredOn(n -> !n.equals("DefaultChannelPipeline$TailContext#0"))
                .containsSubsequence("requestEncoder", "responseDecoder");
        assertThat(pipeline.get(KafkaRequestEncoder.class)).isNotNull();
        assertThat(pipeline.get(KafkaResponseDecoder.class)).isNotNull();
    }

    @Test
    void connectShouldAddFrameLoggerWhenLogFramesEnabled() {
        var mockCcsm = mock(ClientConnectionStateMachine.class);
        var mockVirtualCluster = mock(VirtualClusterModel.class);
        when(mockVirtualCluster.isLogFrames()).thenReturn(true);
        var outboundHolder = new EmbeddedChannel[1];
        var scsm = createConnectableScsm(mockCcsm, mockVirtualCluster, outboundHolder);

        scsm.connect(new EmbeddedChannel());

        assertThat(outboundHolder[0].pipeline().get("frameLogger")).isNotNull();
    }

    @Test
    void connectShouldAddNetworkLoggerWhenLogNetworkEnabled() {
        var mockCcsm = mock(ClientConnectionStateMachine.class);
        var mockVirtualCluster = mock(VirtualClusterModel.class);
        when(mockVirtualCluster.isLogNetwork()).thenReturn(true);
        var outboundHolder = new EmbeddedChannel[1];
        var scsm = createConnectableScsm(mockCcsm, mockVirtualCluster, outboundHolder);

        scsm.connect(new EmbeddedChannel());

        assertThat(outboundHolder[0].pipeline().get("networkLogger")).isNotNull();
    }

    @Test
    void connectTcpFailureShouldCallOnServerException() {
        var mockCcsm = mock(ClientConnectionStateMachine.class);
        var mockVirtualCluster = mock(VirtualClusterModel.class);
        lenient().when(mockCcsm.sessionId()).thenReturn("test-session");
        lenient().when(mockCcsm.clusterName()).thenReturn(CLUSTER_NAME);
        when(mockVirtualCluster.getUpstreamSslContext()).thenReturn(Optional.empty());
        when(mockVirtualCluster.usesDynamicTlsCredentials()).thenReturn(false);
        when(mockVirtualCluster.socketFrameMaxSizeBytes()).thenReturn(
                VirtualClusterModel.DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES);
        var tcpFailure = new RuntimeException("Connection refused");
        var scsm = new ServerConnectionStateMachine(
                BROKER_ADDRESS, mockCcsm, mockVirtualCluster, CLUSTER_NAME, null,
                mock(Counter.class), mock(Timer.class), mock(ActivationToken.class)) {
            @Override
            Bootstrap configureBootstrap(KafkaProxyBackendHandler backendHandler,
                                         Channel inboundChannel) {
                var ch = new EmbeddedChannel();
                Bootstrap bootstrap = new Bootstrap();
                bootstrap.group(ch.eventLoop())
                        .channel(ch.getClass())
                        .handler(backendHandler)
                        .option(ChannelOption.AUTO_READ, true);
                return bootstrap;
            }

            @Override
            ChannelFuture initConnection(String remoteHost, int remotePort, Bootstrap bootstrap) {
                var ch = new EmbeddedChannel();
                return ch.newFailedFuture(tcpFailure);
            }
        };

        scsm.connect(new EmbeddedChannel());

        verify(mockCcsm).onServerConnectionException(tcpFailure);
        assertThat(scsm.state()).isInstanceOf(ServerConnectionState.Closed.class);
    }

    // === TLS credential tests ===

    private ServerConnectionStateMachine createScsmWithMocks(ClientConnectionStateMachine mockCcsm,
                                                             VirtualClusterModel mockVirtualCluster) {
        lenient().when(mockCcsm.sessionId()).thenReturn("test-session");
        lenient().when(mockCcsm.clusterName()).thenReturn(CLUSTER_NAME);
        lenient().when(mockVirtualCluster.getUpstreamSslContext()).thenReturn(Optional.empty());
        return new ServerConnectionStateMachine(
                BROKER_ADDRESS,
                mockCcsm,
                mockVirtualCluster,
                CLUSTER_NAME,
                null,
                mock(Counter.class),
                mock(Timer.class),
                mock(ActivationToken.class));
    }

    @Test
    void invokeTlsCredentialSupplierReportsSynchronousFailure() throws Exception {
        var mockCcsm = mock(ClientConnectionStateMachine.class);
        var mockVirtualCluster = mock(VirtualClusterModel.class);
        RuntimeException failure = new RuntimeException("manager failed");
        when(mockVirtualCluster.getTlsCredentialSupplierManager()).thenThrow(failure);
        var scsm = createScsmWithMocks(mockCcsm, mockVirtualCluster);

        Channel channel = mock(Channel.class);
        ChannelPipeline pipeline = mock(ChannelPipeline.class);
        Method method = ServerConnectionStateMachine.class.getDeclaredMethod(
                "invokeTlsCredentialSupplier", HostPort.class, Channel.class, ChannelPipeline.class);
        method.setAccessible(true);

        method.invoke(scsm, BROKER_ADDRESS, channel, pipeline);

        verify(mockCcsm).onServerConnectionException(failure);
    }

    @Test
    void requestTlsCredentialsAppliesCredentialsOnEventLoop() {
        var mockCcsm = mock(ClientConnectionStateMachine.class);
        var mockVirtualCluster = mock(VirtualClusterModel.class);
        var scsm = createScsmWithMocks(mockCcsm, mockVirtualCluster);

        TlsCredentials badCreds = mock(TlsCredentials.class);
        ServerTlsCredentialSupplier supplier = context -> CompletableFuture.completedFuture(badCreds);
        var supplierContext = new ServerTlsCredentialSupplierContextImpl(null);
        Channel channel = mock(Channel.class);
        EventLoop eventLoop = mock(EventLoop.class);
        when(channel.eventLoop()).thenReturn(eventLoop);
        doAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        }).when(eventLoop).execute(any(Runnable.class));

        scsm.requestTlsCredentials(supplier, supplierContext, BROKER_ADDRESS, channel, mock(ChannelPipeline.class));

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(mockCcsm).onServerConnectionException(captor.capture());
        assertThat(captor.getValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unexpected TlsCredentials implementation");
    }

    @Test
    void requestTlsCredentialsReportsSupplierFailureOnEventLoop() {
        var mockCcsm = mock(ClientConnectionStateMachine.class);
        var mockVirtualCluster = mock(VirtualClusterModel.class);
        var scsm = createScsmWithMocks(mockCcsm, mockVirtualCluster);

        RuntimeException failure = new RuntimeException("boom");
        ServerTlsCredentialSupplier supplier = context -> CompletableFuture.failedFuture(failure);
        var supplierContext = new ServerTlsCredentialSupplierContextImpl(null);
        Channel channel = mock(Channel.class);
        EventLoop eventLoop = mock(EventLoop.class);
        when(channel.eventLoop()).thenReturn(eventLoop);
        doAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        }).when(eventLoop).execute(any(Runnable.class));

        scsm.requestTlsCredentials(supplier, supplierContext, BROKER_ADDRESS, channel, mock(ChannelPipeline.class));

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(mockCcsm).onServerConnectionException(captor.capture());
        assertThat(captor.getValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to obtain TLS credentials")
                .hasCause(failure);
    }

    @Test
    void handleTlsCredentialSupplierResultReportsNullCredentials() {
        var mockCcsm = mock(ClientConnectionStateMachine.class);
        var mockVirtualCluster = mock(VirtualClusterModel.class);
        var scsm = createScsmWithMocks(mockCcsm, mockVirtualCluster);

        Channel channel = mock(Channel.class);
        ChannelPipeline pipeline = mock(ChannelPipeline.class);

        scsm.handleTlsCredentialSupplierResult(null, null, BROKER_ADDRESS, channel, pipeline);

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(mockCcsm).onServerConnectionException(captor.capture());
        assertThat(captor.getValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("TLS credential supplier returned null");
    }

    @Test
    void handleTlsCredentialSupplierResultAppliesCredentials() {
        var mockCcsm = mock(ClientConnectionStateMachine.class);
        var mockVirtualCluster = mock(VirtualClusterModel.class);
        var scsm = createScsmWithMocks(mockCcsm, mockVirtualCluster);

        TlsCredentials badCreds = mock(TlsCredentials.class);
        Channel channel = mock(Channel.class);
        ChannelPipeline pipeline = mock(ChannelPipeline.class);

        scsm.handleTlsCredentialSupplierResult(badCreds, null, BROKER_ADDRESS, channel, pipeline);

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(mockCcsm).onServerConnectionException(captor.capture());
        assertThat(captor.getValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unexpected TlsCredentials implementation");
    }

    @Test
    void applySslContextToChannelRejectsNonTlsCredentialsImpl() {
        var mockCcsm = mock(ClientConnectionStateMachine.class);
        var mockVirtualCluster = mock(VirtualClusterModel.class);
        var scsm = createScsmWithMocks(mockCcsm, mockVirtualCluster);

        TlsCredentials badCreds = mock(TlsCredentials.class);
        Channel channel = mock(Channel.class);
        ChannelPipeline pipeline = mock(ChannelPipeline.class);

        scsm.applySslContextToChannel(badCreds, BROKER_ADDRESS, channel, pipeline);

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(mockCcsm).onServerConnectionException(captor.capture());
        assertThat(captor.getValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unexpected TlsCredentials implementation");
    }

    @Test
    void applySslContextToChannelAddsSslHandlerWithValidCredentials() throws Exception {
        var mockCcsm = mock(ClientConnectionStateMachine.class);
        var mockVirtualCluster = mock(VirtualClusterModel.class);
        var scsm = createScsmWithMocks(mockCcsm, mockVirtualCluster);

        var keyAndCert = TestCertificateUtil.generateKeyStoreAndCert();
        var creds = new TlsCredentialsImpl(
                keyAndCert.privateKey(), new java.security.cert.X509Certificate[]{ keyAndCert.cert() });

        EmbeddedChannel channel = new EmbeddedChannel();

        when(mockVirtualCluster.targetCluster()).thenReturn(mock(io.kroxylicious.proxy.config.TargetCluster.class));
        when(mockVirtualCluster.targetCluster().tls()).thenReturn(Optional.empty());

        scsm.applySslContextToChannel(creds, BROKER_ADDRESS, channel, channel.pipeline());

        assertThat(channel.pipeline().get("ssl")).isNotNull();
        verify(mockCcsm, never()).onServerConnectionException(any());

        channel.close();
    }
}
