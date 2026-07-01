/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import javax.net.ssl.SSLSession;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.ScheduledFuture;

import io.kroxylicious.proxy.bootstrap.RouterChainFactory;
import io.kroxylicious.proxy.config.CacheConfiguration;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.net.HaProxyContext;
import io.kroxylicious.proxy.internal.routing.DirectRouting;
import io.kroxylicious.proxy.internal.routing.DynamicRouting;
import io.kroxylicious.proxy.internal.routing.RouteDescriptor;
import io.kroxylicious.proxy.internal.subject.DefaultSubjectBuilder;
import io.kroxylicious.proxy.internal.util.VirtualClusterNode;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClientConnectionStateMachineTest {

    private static final HostPort BROKER_ADDRESS = new HostPort("localhost", 9092);
    private static final HaProxyContext HA_PROXY_CONTEXT = new HaProxyContext("1.1.1.1", "2.2.2.2", 46421, 9092, java.util.Map.of());
    private static final Offset<Double> CLOSE_ENOUGH = Offset.offset(0.00005);
    private static final String CLUSTER_NAME = "virtualClusterA";
    private static final VirtualClusterNode VIRTUAL_CLUSTER_NODE = new VirtualClusterNode(CLUSTER_NAME, null);
    private static final VirtualClusterModel VIRTUAL_CLUSTER_MODEL = new VirtualClusterModel(CLUSTER_NAME,
            new DirectRouting(new TargetCluster("", Optional.empty())), false, false,
            List.of(), CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null);
    public static final KafkaSession TEST_KAFKA_SESSION = new KafkaSession("testSession", KafkaSessionState.NOT_AUTHENTICATED);
    private final RuntimeException failure = new RuntimeException("There's Klingons on the starboard bow");
    private ClientConnectionStateMachine clientConnectionStateMachine;

    @Mock
    private EndpointBinding endpointBinding;
    @Mock
    private EndpointGateway endpointGateway;

    @Mock
    private ServerConnectionStateMachine serverConnectionStateMachine;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private KafkaProxyFrontendHandler frontendHandler;
    private SimpleMeterRegistry simpleMeterRegistry;

    @BeforeEach
    void setUp() {
        simpleMeterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(simpleMeterRegistry);
        when(endpointBinding.nodeId()).thenReturn(null);
        when(endpointBinding.endpointGateway()).thenReturn(endpointGateway);
        when(endpointGateway.virtualCluster()).thenReturn(VIRTUAL_CLUSTER_MODEL);
        clientConnectionStateMachine = new ClientConnectionStateMachine(endpointBinding, new DefaultSubjectBuilder(List.of()),
                new KafkaSession(KafkaSessionState.ESTABLISHING),
                (remote, ccsm, vc, cn, ni, connectionCounter, errorCounter, backpressureMeter, connectionToken, tlsConfig) -> serverConnectionStateMachine);
        when(frontendHandler.channelId()).thenReturn(DefaultChannelId.newInstance());
        when(frontendHandler.remoteHost()).thenReturn("testhost.example.com");
        when(frontendHandler.remotePort()).thenReturn(9476);
        when(frontendHandler.clientChannel()).thenReturn(mock(Channel.class));
        // Make the executor run tasks synchronously for tests
        when(frontendHandler.eventLoopExecutor()).thenReturn(Runnable::run);
        lenient().when(serverConnectionStateMachine.isWritable()).thenReturn(true);
    }

    @AfterEach
    void tearDown() {
        if (simpleMeterRegistry != null) {
            simpleMeterRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
            Metrics.globalRegistry.remove(simpleMeterRegistry);
        }
    }

    @Test
    void shouldCountClientToProxyConnections() {
        // Given

        // When
        clientConnectionStateMachine.onClientActive(frontendHandler);

        // Then
        assertThat(Metrics.globalRegistry.get("kroxylicious_client_to_proxy_connections").counter())
                .isNotNull()
                .satisfies(counter -> assertThat(counter.getId()).isNotNull())
                .satisfies(counter -> assertThat(counter.count())
                        .isCloseTo(1.0, CLOSE_ENOUGH));
    }

    @ParameterizedTest
    @MethodSource("clientErrorStates")
    void shouldCountClientToProxyExceptions(Runnable givenState, Boolean tlsEnabled) {
        // Given
        givenState.run();

        // When
        clientConnectionStateMachine.onClientException(failure);

        // Then
        assertThat(Metrics.globalRegistry.get("kroxylicious_client_to_proxy_errors").counter())
                .isNotNull()
                .satisfies(counter -> assertThat(counter.getId()).isNotNull())
                .satisfies(counter -> assertThat(counter.count())
                        .isCloseTo(1.0, CLOSE_ENOUGH));
    }

    @ParameterizedTest
    @MethodSource("givenStates")
    void shouldTransitionToClosedOnServerException(Runnable givenState) {
        // Given
        givenState.run();

        // When
        clientConnectionStateMachine.onServerConnectionException(failure);

        // Then — server error counting is now the SCSM's concern; CCSM just transitions to Closed
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
    }

    @Test
    void shouldPassConnectionCounterToScsmFactory() {
        // Given
        var capturedCounter = new java.util.concurrent.atomic.AtomicReference<io.micrometer.core.instrument.Counter>();
        var ccsm = new ClientConnectionStateMachine(endpointBinding, new DefaultSubjectBuilder(List.of()),
                new KafkaSession(KafkaSessionState.ESTABLISHING),
                (remote, c, vc, cn, ni, connectionCounter, errorCounter, backpressureMeter, connectionToken, tlsConfig) -> {
                    capturedCounter.set(connectionCounter);
                    return serverConnectionStateMachine;
                });
        ccsm.forceState(new ClientConnectionState.ClientActive(), frontendHandler,
                Map.of(), TEST_KAFKA_SESSION, true);
        when(endpointBinding.upstreamTarget()).thenReturn(BROKER_ADDRESS);

        // When
        ccsm.onClientRequest(metadataRequest());

        // Then: the counter passed is the Micrometer counter registered for proxyToServer connections
        assertThat(capturedCounter.get()).isNotNull();
        assertThat(Metrics.globalRegistry.get("kroxylicious_proxy_to_server_connections").counter())
                .isNotNull();
    }

    @Test
    void shouldTransitionToClosedOnServerExceptionInForwardingAwaitingBackend() {
        // Given
        stateMachineInForwardingAwaitingTransportSubject();

        // When
        clientConnectionStateMachine.onServerConnectionException(failure);

        // Then — server error counting is now the SCSM's concern; CCSM just transitions to Closed
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
    }

    @Test
    void shouldBlockClientReads() {
        // Given
        stateMachineInClientActive();
        clientConnectionStateMachine.onServerUnwritable();

        // When
        clientConnectionStateMachine.onServerUnwritable();

        // Then
        verify(frontendHandler, times(1)).applyBackpressure();
    }

    @Test
    void shouldUnblockClientReads() {
        // Given
        stateMachineInClientActive();
        clientConnectionStateMachine.clientReadsBlocked = true;
        clientConnectionStateMachine.onServerWritable();

        // When
        clientConnectionStateMachine.onServerWritable();

        // Then
        verify(frontendHandler, times(1)).relieveBackpressure();
    }

    @Test
    void shouldBlockServerReads() {
        // Given
        stateMachineInForwarding();
        clientConnectionStateMachine.onClientUnwritable();

        // When
        clientConnectionStateMachine.onClientUnwritable();

        // Then — CCSM delegates every call; idempotency is the SCSM's concern
        verify(serverConnectionStateMachine, times(2)).applyBackpressure();
    }

    @Test
    void shouldCloseOnClientRuntimeException() {
        // Given
        stateMachineInForwarding();
        RuntimeException cause = new RuntimeException("Oops!");

        useDownstreamSsl();

        // When
        clientConnectionStateMachine.onClientException(cause);

        // Then
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
        verify(serverConnectionStateMachine).close();
        verify(frontendHandler).inClosed(ArgumentMatchers.notNull(UnknownServerException.class));
    }

    private void useDownstreamSsl() {
        SslContext mock = mock(SslContext.class);
        when(clientConnectionStateMachine.endpointGateway().getDownstreamSslContext()).thenReturn(Optional.of(mock));
    }

    @Test
    void shouldCloseOnClientFrameOversizedException() {
        // Given
        stateMachineInForwarding();
        RuntimeException cause = new DecoderException(new FrameOversizedException(2, 1));
        useDownstreamSsl();

        // When
        clientConnectionStateMachine.onClientException(cause);

        // Then
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
        verify(serverConnectionStateMachine).close();
        verify(frontendHandler).inClosed(ArgumentMatchers.notNull(InvalidRequestException.class));
    }

    @Test
    void shouldCloseOnServerRuntimeException() {
        // Given
        stateMachineInForwarding();
        RuntimeException cause = new RuntimeException("Oops!");

        // When
        clientConnectionStateMachine.onServerConnectionException(cause);

        // Then
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
        verify(serverConnectionStateMachine).close();
        verify(frontendHandler).inClosed(cause);
    }

    @Test
    void shouldUnblockServerReads() {
        // Given
        stateMachineInForwarding();

        // When
        clientConnectionStateMachine.onClientWritable();

        // Then
        verify(serverConnectionStateMachine).relieveBackpressure();
    }

    @Test
    void shouldNotifyHandlerOnTransitionFromStartToClientActive() {
        // Given

        // When
        clientConnectionStateMachine.onClientActive(frontendHandler);

        // Then
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.ClientActive.class);
        verify(frontendHandler, times(1)).inClientActive();
    }

    @Test
    void onClientActiveShouldTransitionToHaProxyWhenContextPresentInSession() {
        // Given - HaProxy context stored in KafkaSession (by HaProxyMessageHandler before CCSM was created)
        clientConnectionStateMachine.kafkaSession().setHaProxyContext(HA_PROXY_CONTEXT);

        // When
        clientConnectionStateMachine.onClientActive(frontendHandler);

        // Then - state machine transitions through ClientActive → HaProxy
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.HaProxy.class);
        assertThat(clientConnectionStateMachine.kafkaSession().haProxyContext()).isNotNull();
        verify(frontendHandler).inClientActive();
    }

    @Test
    void inClientActiveShouldBufferAndTransitionToForwardingWhenOnClientMetadataRequest() {
        // Given
        stateMachineInClientActive();
        when(endpointBinding.upstreamTarget()).thenReturn(BROKER_ADDRESS);
        var msg = metadataRequest();

        // When
        clientConnectionStateMachine.onClientRequest(msg);

        // Then
        assertThat(clientConnectionStateMachine.state())
                .isInstanceOf(ClientConnectionState.Forwarding.class);
        verify(frontendHandler).bufferMsg(msg);
        verify(serverConnectionStateMachine).connect(notNull(Channel.class));
    }

    @Test
    void inHaProxyShouldBufferAndTransitionToForwardingWhenOnClientApiVersionsRequest() {
        // Given
        stateMachineInHaProxy();
        when(endpointBinding.upstreamTarget()).thenReturn(BROKER_ADDRESS);
        var msg = apiVersionsRequest();

        // When
        clientConnectionStateMachine.onClientRequest(msg);

        // Then
        assertThat(clientConnectionStateMachine.state())
                .isInstanceOf(ClientConnectionState.Forwarding.class);
        verify(frontendHandler).bufferMsg(msg);
        verify(serverConnectionStateMachine).connect(notNull(Channel.class));
    }

    @Test
    void inHaProxyShouldCloseOnUnexpectedMessage() {
        // Given
        stateMachineInHaProxy();

        // When - an unexpected (non-Kafka) message arrives
        clientConnectionStateMachine.onClientRequest(new Object());

        // Then
        assertThat(clientConnectionStateMachine.state())
                .isInstanceOf(ClientConnectionState.Closed.class);
        verify(frontendHandler).inClosed(null);
    }

    @Test
    void inHaProxyShouldBufferAndTransitionToForwardingWhenOnClientMetadataRequest() {
        // Given
        stateMachineInHaProxy();
        when(endpointBinding.upstreamTarget()).thenReturn(BROKER_ADDRESS);
        var msg = metadataRequest();

        // When
        clientConnectionStateMachine.onClientRequest(msg);

        // Then
        assertThat(clientConnectionStateMachine.state())
                .isInstanceOf(ClientConnectionState.Forwarding.class);
        verify(frontendHandler).bufferMsg(msg);
        verify(serverConnectionStateMachine).connect(notNull(Channel.class));
    }

    @Test
    void inClientActiveShouldCaptureClientSoftwareInfoFromApiVersions() {
        // Given
        stateMachineInClientActive();
        when(endpointBinding.upstreamTarget()).thenReturn(BROKER_ADDRESS);
        var msg = apiVersionsRequest();

        // When
        clientConnectionStateMachine.onClientRequest(msg);

        // Then
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Forwarding.class);
        assertThat(clientConnectionStateMachine.clientSoftwareName()).isEqualTo("mykafkalib");
        assertThat(clientConnectionStateMachine.clientSoftwareVersion()).isEqualTo("1.0.0");
        verify(frontendHandler).bufferMsg(msg);
    }

    @Test
    void onServerActiveShouldNotUnblockClient() {
        // Given — Forwarding state, transport subject not yet ready
        clientConnectionStateMachine.forceState(
                new ClientConnectionState.Forwarding(),
                frontendHandler,
                Map.of(BROKER_ADDRESS, serverConnectionStateMachine),
                TEST_KAFKA_SESSION,
                false);

        // When
        clientConnectionStateMachine.onServerConnectionActive();

        // Then — backend activation no longer participates in unblocking
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Forwarding.class);
        verify(frontendHandler, never()).unblockClient();
    }

    @Test
    void inForwardingShouldBufferRequestsWhenTransportSubjectNotReady() {
        // Given — Forwarding state with latch > 0 (backend not yet connected)
        stateMachineInForwardingAwaitingTransportSubject();

        // When
        DecodedRequestFrame<MetadataRequestData> msg = metadataRequest();
        clientConnectionStateMachine.onClientRequest(msg);

        // Then
        verify(frontendHandler).bufferMsg(msg);
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Forwarding.class);
    }

    @Test
    void inClientActiveShouldCloseWhenOnServerActiveCalled() {
        // Given
        stateMachineInClientActive();

        // When
        clientConnectionStateMachine.onServerConnectionActive();

        // Then
        assertThat(clientConnectionStateMachine.state())
                .isInstanceOf(ClientConnectionState.Closed.class);
        verify(frontendHandler).inClosed(null);
    }

    @Test
    void inForwardingShouldForwardClientRequests() {
        // Given
        var serverCtx = mock(ChannelHandlerContext.class);
        var forwarding = stateMachineInForwarding();
        var msg = metadataRequest();

        // When
        clientConnectionStateMachine.onClientRequest(msg);

        // Then
        assertThat(clientConnectionStateMachine.state()).isSameAs(forwarding);
        verify(frontendHandler).admitToFilterChain(msg);
        verifyNoInteractions(serverCtx);
    }

    @Test
    void inForwardingShouldForwardServerResponses() {
        // Given
        var serverCtx = mock(ChannelHandlerContext.class);
        var forwarding = stateMachineInForwarding();
        var msg = metadataResponse();

        // When
        clientConnectionStateMachine.onResponseFromServer(msg);

        // Then
        assertThat(clientConnectionStateMachine.state()).isSameAs(forwarding);
        verify(frontendHandler).forwardToClient(msg);
        verifyNoInteractions(serverCtx);
        verifyNoInteractions(serverConnectionStateMachine);
    }

    @Test
    void inForwardingShouldTransitionToClosedOnServerInactive() {
        // Given
        stateMachineInForwarding();
        doAnswer(invocation -> assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class)).when(frontendHandler).inClosed(null);
        doNothing().when(serverConnectionStateMachine).close();

        // When
        clientConnectionStateMachine.onServerConnectionClosed(ClientConnectionStateMachine.DisconnectCause.SERVER_CLOSED);

        // Then
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
        verify(frontendHandler).inClosed(null);
        verify(serverConnectionStateMachine).close();
    }

    @Test
    void inForwardingShouldTransitionToClosedOnClientInactive() {
        // Given
        stateMachineInForwarding();
        doAnswer(invocation -> assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class)).when(frontendHandler).inClosed(null);

        // When
        clientConnectionStateMachine.onClientInactive();

        // Then
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
        verify(frontendHandler).inClosed(null);
        verify(serverConnectionStateMachine).close();
    }

    @Test
    void inForwardingShouldTransitionToClosedOnClientIdle() {
        // Given
        stateMachineInForwarding();
        doAnswer(invocation -> assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class)).when(frontendHandler).inClosed(null);

        // When
        clientConnectionStateMachine.onClientIdle();

        // Then
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
        assertThat(clientConnectionStateMachine.kafkaSession().currentState()).isEqualTo(KafkaSessionState.TERMINATING);
        verify(frontendHandler).inClosed(null);
        verify(serverConnectionStateMachine).close();
    }

    @Test
    void shouldNotTransitionToClosedMultipleTimes() {
        // Given
        stateMachineInClosed();

        // When
        clientConnectionStateMachine.onServerConnectionClosed(ClientConnectionStateMachine.DisconnectCause.SERVER_CLOSED);

        // Then
        verifyNoInteractions(frontendHandler, serverConnectionStateMachine);
    }

    @Test
    void closingShouldCloseAllScsmAndNullRouteTargets() {
        // Given: routing VC in Forwarding with two SCSMs
        var scsm1 = mock(ServerConnectionStateMachine.class);
        var scsm2 = mock(ServerConnectionStateMachine.class);
        var addr1 = new HostPort("host1", 9092);
        var addr2 = new HostPort("host2", 9092);
        var forwarding = new ClientConnectionState.Forwarding();
        clientConnectionStateMachine.forceState(
                forwarding,
                frontendHandler,
                Map.of(addr1, scsm1, addr2, scsm2),
                TEST_KAFKA_SESSION,
                true,
                Map.of("route-a", addr1, "route-b", addr2));

        // When: close is triggered
        clientConnectionStateMachine.onServerConnectionException(failure);

        // Then: both SCSMs are closed and state is Closed
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
        verify(scsm1).close();
        verify(scsm2).close();
    }

    @Test
    void inForwardingShouldTransitionToClosedOnServerException() {
        // Given
        stateMachineInForwarding();
        final IllegalStateException illegalStateException = new IllegalStateException("She canny take it any more, captain");
        doNothing().when(serverConnectionStateMachine).close();

        // When
        clientConnectionStateMachine.onServerConnectionException(illegalStateException);

        // Then
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
        verify(frontendHandler).inClosed(illegalStateException);
        verify(serverConnectionStateMachine).close();
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void inForwardingShouldTransitionToClosedOnClientException(boolean tlsEnabled) {
        // Given
        stateMachineInForwarding();
        final ApiException expectedException = Errors.UNKNOWN_SERVER_ERROR.exception();
        final IllegalStateException illegalStateException = new IllegalStateException("She canny take it any more, captain");
        doAnswer(invocation -> assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class)).when(frontendHandler)
                .inClosed(expectedException);
        doNothing().when(serverConnectionStateMachine).close();
        if (tlsEnabled) {
            useDownstreamSsl();
        }

        // When
        clientConnectionStateMachine.onClientException(illegalStateException);

        // Then
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
        verify(frontendHandler).inClosed(expectedException);
        verify(serverConnectionStateMachine).close();
    }

    @ParameterizedTest
    @MethodSource("connectedStates")
    void shouldDelegateServerBackpressureToScsmOnClientUnwritable(Runnable givenState) {
        // Given
        givenState.run();

        // When
        clientConnectionStateMachine.onClientUnwritable();

        // Then
        verify(serverConnectionStateMachine).applyBackpressure();
    }

    @ParameterizedTest
    @MethodSource("connectedStates")
    void shouldDelegateServerBackpressureReliefToScsmOnClientWritable(Runnable givenState) {
        // Given
        givenState.run();

        // When
        clientConnectionStateMachine.onClientWritable();

        // Then
        verify(serverConnectionStateMachine).relieveBackpressure();
    }

    @ParameterizedTest
    @MethodSource("givenStates")
    void shouldStartClientTimerWhenServerIsUnwritable(Runnable givenState) {
        // Given
        givenState.run();

        // When
        clientConnectionStateMachine.onServerUnwritable();

        // Then
        assertThat(clientConnectionStateMachine.clientToProxyBackpressureTimer)
                .isInstanceOf(Timer.Sample.class);
    }

    @ParameterizedTest
    @MethodSource("givenStates")
    void shouldStopClientTimerWhenServerIsWritable(Runnable givenState) {
        // Given
        givenState.run();
        clientConnectionStateMachine.onServerUnwritable();

        // When
        clientConnectionStateMachine.onServerWritable();

        // Then
        assertThat(Metrics.globalRegistry.get("kroxylicious_client_to_proxy_reads_paused").timer())
                .isInstanceOf(Timer.class)
                .satisfies(timer -> assertThat(timer.count()).isGreaterThanOrEqualTo(1)
                // Count is incremented when the timer is stopped
                );
        assertThat(clientConnectionStateMachine.clientToProxyBackpressureTimer).isNull();
    }

    @Test
    void shouldNotifyFrontendHandlerThatAuthenticationHasCompleted() {
        // Given
        stateMachineInForwarding();

        // When
        clientConnectionStateMachine.onSessionSaslAuthenticated();

        // Then
        verify(frontendHandler).onSessionAuthenticated();
    }

    public Stream<Arguments> clientErrorStates() {
        return Stream.of(
                argumentSet("STARTING TLS on", (Runnable) () -> {
                    // no Op
                }, true),
                argumentSet("STARTING TLS off ", (Runnable) () -> {
                    // no Op
                }, false),
                argumentSet("Ha Proxy TLS on", (Runnable) this::stateMachineInHaProxy, true),
                argumentSet("Ha Proxy TLS off ", (Runnable) this::stateMachineInHaProxy, false),
                argumentSet("Forwarding awaiting backend TLS on", (Runnable) this::stateMachineInForwardingAwaitingTransportSubject, true),
                argumentSet("Forwarding awaiting backend TLS off ", (Runnable) this::stateMachineInForwardingAwaitingTransportSubject, false),
                argumentSet("Client Active TLS on", (Runnable) this::stateMachineInClientActive, true),
                argumentSet("Client Active TLS off ", (Runnable) this::stateMachineInClientActive, false),
                argumentSet("Forwarding TLS on", (Runnable) this::stateMachineInForwarding, true),
                argumentSet("Forwarding TLS off ", (Runnable) this::stateMachineInForwarding, false),
                argumentSet("Closed TLS on", (Runnable) this::stateMachineInClosed, true),
                argumentSet("Closed TLS off ", (Runnable) this::stateMachineInClosed, false));
    }

    public Stream<Arguments> givenStates() {
        return Stream.of(
                argumentSet("Ha Proxy", (Runnable) this::stateMachineInHaProxy),
                argumentSet("Forwarding awaiting backend", (Runnable) this::stateMachineInForwardingAwaitingTransportSubject),
                argumentSet("ClientActive ", (Runnable) this::stateMachineInClientActive),
                argumentSet("Forwarding", (Runnable) this::stateMachineInForwarding),
                argumentSet("Closed", (Runnable) this::stateMachineInClosed));
    }

    public Stream<Arguments> connectedStates() {
        return Stream.of(
                argumentSet("Forwarding awaiting backend", (Runnable) this::stateMachineInForwardingAwaitingTransportSubject),
                argumentSet("Forwarding", (Runnable) this::stateMachineInForwarding),
                argumentSet("Closed", (Runnable) this::stateMachineInClosed));
    }

    private void stubAsRouterVirtualCluster() {
        var routerVc = mock(VirtualClusterModel.class, withSettings().lenient());
        var routeDescriptors = Map.of("route",
                new RouteDescriptor("route", 0, new TargetCluster("broker:9092", Optional.empty()), null, List.of()));
        when(routerVc.routing()).thenReturn(new DynamicRouting("router", routeDescriptors, mock(RouterChainFactory.class)));
        when(endpointGateway.virtualCluster()).thenReturn(routerVc);
    }

    private void stateMachineInClientActive() {
        clientConnectionStateMachine.forceState(
                new ClientConnectionState.ClientActive(),
                frontendHandler,
                Map.of(),
                TEST_KAFKA_SESSION,
                true);
    }

    private void stateMachineInHaProxy() {
        clientConnectionStateMachine.forceState(
                new ClientConnectionState.HaProxy(),
                frontendHandler,
                Map.of(),
                TEST_KAFKA_SESSION,
                true);
    }

    private ClientConnectionState.Forwarding stateMachineInForwarding() {
        var forwarding = new ClientConnectionState.Forwarding();
        clientConnectionStateMachine.forceState(
                forwarding,
                frontendHandler,
                Map.of(BROKER_ADDRESS, serverConnectionStateMachine),
                TEST_KAFKA_SESSION,
                true);
        return forwarding;
    }

    private void stateMachineInForwardingAwaitingTransportSubject() {
        clientConnectionStateMachine.forceState(
                new ClientConnectionState.Forwarding(),
                frontendHandler,
                Map.of(BROKER_ADDRESS, serverConnectionStateMachine),
                TEST_KAFKA_SESSION,
                false);
    }

    private void stateMachineInClosed() {
        clientConnectionStateMachine.forceState(
                new ClientConnectionState.Closed(),
                frontendHandler,
                Map.of(BROKER_ADDRESS, serverConnectionStateMachine),
                TEST_KAFKA_SESSION,
                true);
    }

    private static DecodedRequestFrame<ApiVersionsRequestData> apiVersionsRequest() {
        return new DecodedRequestFrame<>(
                ApiVersionsResponseData.ApiVersion.HIGHEST_SUPPORTED_VERSION,
                1,
                false,
                new RequestHeaderData(),
                new ApiVersionsRequestData()
                        .setClientSoftwareName("mykafkalib")
                        .setClientSoftwareVersion("1.0.0"));
    }

    private static DecodedRequestFrame<MetadataRequestData> metadataRequest() {
        return new DecodedRequestFrame<>(
                MetadataRequestData.HIGHEST_SUPPORTED_VERSION,
                0,
                false,
                new RequestHeaderData(),
                new MetadataRequestData());
    }

    private static DecodedResponseFrame<MetadataResponseData> metadataResponse() {
        return new DecodedResponseFrame<>(
                MetadataRequestData.HIGHEST_SUPPORTED_VERSION,
                0,
                new ResponseHeaderData(),
                new MetadataResponseData());
    }

    @Test
    void shouldIncrementClientToProxyActiveConnectionsOnClientActive() {
        // Given
        int initialCount = getVirtualNodeClientToProxyActiveConnections();

        // When
        clientConnectionStateMachine.onClientActive(frontendHandler);

        // Then
        assertThat(getVirtualNodeClientToProxyActiveConnections())
                .isEqualTo(initialCount + 1);
    }

    @Test
    void shouldRemainInForwardingWhenOnServerConnectionActive() {
        // Given
        stateMachineInForwardingAwaitingTransportSubject();

        // When
        clientConnectionStateMachine.onServerConnectionActive();

        // Then — state is still Forwarding (latch decremented but not zero)
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Forwarding.class);
    }

    @Test
    void shouldDecrementActiveConnectionsOnClosed() {
        // Given - establish both client and server connections
        clientConnectionStateMachine.onClientActive(frontendHandler);
        stateMachineInForwardingAwaitingTransportSubject();
        clientConnectionStateMachine.onServerConnectionActive();

        int initialClientCount = getVirtualNodeClientToProxyActiveConnections();

        // When
        clientConnectionStateMachine.onClientInactive();

        // Then — server connection metric is now the SCSM's concern
        assertThat(getVirtualNodeClientToProxyActiveConnections())
                .isEqualTo(initialClientCount - 1);
        verify(serverConnectionStateMachine).close();
    }

    @Test
    void shouldDecrementActiveConnectionsOnServerInactive() {
        // Given - establish both client and server connections
        clientConnectionStateMachine.onClientActive(frontendHandler);
        stateMachineInForwardingAwaitingTransportSubject();
        clientConnectionStateMachine.onServerConnectionActive();

        int initialClientCount = getVirtualNodeClientToProxyActiveConnections();

        // When
        clientConnectionStateMachine.onServerConnectionClosed(ClientConnectionStateMachine.DisconnectCause.SERVER_CLOSED);

        // Then — server connection metric is now the SCSM's concern
        assertThat(getVirtualNodeClientToProxyActiveConnections())
                .isEqualTo(initialClientCount - 1);
    }

    @Test
    void shouldDecrementActiveConnectionsOnClientException() {
        // Given - establish client connection
        clientConnectionStateMachine.onClientActive(frontendHandler);
        int initialClientCount = getVirtualNodeClientToProxyActiveConnections();

        // When
        clientConnectionStateMachine.onClientException(new RuntimeException("test exception"));

        // Then
        assertThat(getVirtualNodeClientToProxyActiveConnections())
                .isEqualTo(initialClientCount - 1);
    }

    @Test
    void shouldDecrementActiveConnectionsOnServerException() {
        // Given - establish both client and server connections
        clientConnectionStateMachine.onClientActive(frontendHandler);
        stateMachineInForwardingAwaitingTransportSubject();
        clientConnectionStateMachine.onServerConnectionActive();

        int initialClientCount = getVirtualNodeClientToProxyActiveConnections();

        // When
        clientConnectionStateMachine.onServerConnectionException(new RuntimeException("test exception"));

        // Then — server connection metric is now the SCSM's concern
        assertThat(getVirtualNodeClientToProxyActiveConnections())
                .isEqualTo(initialClientCount - 1);
        verify(serverConnectionStateMachine).close();
    }

    @Test
    void shouldOnlyDecrementClientConnectionsWhenNotInForwardingState() {
        // Given - establish client connection but not server connection
        clientConnectionStateMachine.onClientActive(frontendHandler);
        int initialClientCount = getVirtualNodeClientToProxyActiveConnections();
        int initialServerCount = getVirtualNodeProxyToServerActiveConnections();

        // When - close while not in forwarding state
        clientConnectionStateMachine.onClientInactive();

        // Then - only client connections decremented
        assertThat(getVirtualNodeClientToProxyActiveConnections())
                .isEqualTo(initialClientCount - 1);
        assertThat(getVirtualNodeProxyToServerActiveConnections())
                .isEqualTo(initialServerCount); // unchanged
    }

    @Test
    void shouldFlushToServerWhenClientReadCompletes() {
        // Given
        stateMachineInForwarding();
        Object msg = new Object();

        // When
        clientConnectionStateMachine.onDirectClientFilterChainComplete(msg);

        // Then
        verify(serverConnectionStateMachine).sendRequest(msg);
    }

    @Test
    void onDirectClientFilterChainCompleteNotInForwarding() {
        // Given
        stateMachineInClientActive();
        Object msg = new Object();

        // When
        clientConnectionStateMachine.onDirectClientFilterChainComplete(msg);

        // Then
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
        verify(frontendHandler).inClosed(null);
    }

    @Test
    void toForwardingWithRoutesShouldPopulateRouteTargetsAfterScsmCreation() {
        // Given: a router VC with one route and a healthy SCSM factory
        stateMachineInClientActive();
        var target = new HostPort("broker", 9092);
        var targetCluster = mock(TargetCluster.class, withSettings().lenient());
        when(targetCluster.bootstrapServer()).thenReturn(target);
        var routeDescriptor = mock(RouteDescriptor.class, withSettings().lenient());
        when(routeDescriptor.targetsCluster()).thenReturn(true);
        when(routeDescriptor.targetCluster()).thenReturn(targetCluster);
        var routerVc = mock(VirtualClusterModel.class, withSettings().lenient());
        when(routerVc.routing()).thenReturn(new DynamicRouting("router", Map.of("my-route", routeDescriptor), mock(RouterChainFactory.class)));
        when(endpointGateway.virtualCluster()).thenReturn(routerVc);

        // When: first request triggers toForwardingWithRoutes
        clientConnectionStateMachine.onClientRequest(metadataRequest());

        // Then: forwardToRoute dispatches to the SCSM, proving routeTargets was populated
        var msg = new Object();
        clientConnectionStateMachine.forwardToRoute("my-route", msg);
        verify(serverConnectionStateMachine).sendRequest(msg);
    }

    @Test
    void forwardToRouteShouldTransitionToClosedIfScsmCreationFails() {
        // Given: a CCSM whose SCSM factory throws on creation, in Forwarding state with a known route
        var failingCcsm = new ClientConnectionStateMachine(endpointBinding, new DefaultSubjectBuilder(List.of()),
                new KafkaSession(KafkaSessionState.ESTABLISHING),
                (remote, ccsm, vc, cn, ni, cc, ec, bpm, token, tlsConfig) -> {
                    throw new RuntimeException("scsm creation failed");
                });
        var target = new HostPort("broker", 9092);
        failingCcsm.forceState(new ClientConnectionState.Forwarding(), frontendHandler,
                Map.of(), TEST_KAFKA_SESSION, true, Map.of("my-route", target));
        var routerVc = mock(VirtualClusterModel.class, withSettings().lenient());
        var routeDescriptors2 = Map.of("my-route",
                new RouteDescriptor("my-route", 0, new TargetCluster("broker:9092", Optional.empty()), null, List.of()));
        when(routerVc.routing()).thenReturn(new DynamicRouting("router", routeDescriptors2, mock(RouterChainFactory.class)));
        when(endpointGateway.virtualCluster()).thenReturn(routerVc);

        // When: forwardToRoute lazily tries to create an SCSM and fails
        var msg = new Object();
        assertThatThrownBy(() -> failingCcsm.forwardToRoute("my-route", msg))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("scsm creation failed");
    }

    @Test
    void forwardToRouteShouldThrowWhenVcDoesNotUseRouter() {
        assertThatThrownBy(() -> clientConnectionStateMachine.forwardToRoute("any-route", new Object()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not use a router");
    }

    @Test
    void toForwardingWithRoutesShouldFailFastIfRouteBootstrapServerIsNull() {
        // Given
        stateMachineInClientActive();
        var targetCluster = mock(TargetCluster.class, withSettings().lenient());
        when(targetCluster.bootstrapServer()).thenReturn(null);
        var routeDescriptor = mock(RouteDescriptor.class, withSettings().lenient());
        when(routeDescriptor.targetsCluster()).thenReturn(true);
        when(routeDescriptor.targetCluster()).thenReturn(targetCluster);
        var routerVc = mock(VirtualClusterModel.class, withSettings().lenient());
        when(routerVc.routing()).thenReturn(new DynamicRouting("router", Map.of("bad-route", routeDescriptor), mock(RouterChainFactory.class)));
        when(endpointGateway.virtualCluster()).thenReturn(routerVc);

        // When / Then
        assertThatThrownBy(() -> clientConnectionStateMachine.onClientRequest(metadataRequest()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("bootstrapServer");
    }

    @Test
    void forwardToRouteShouldDispatchToCorrectScsm() {
        // Given
        stubAsRouterVirtualCluster();
        var scsm1 = mock(ServerConnectionStateMachine.class);
        var scsm2 = mock(ServerConnectionStateMachine.class);
        var addr1 = new HostPort("host1", 9092);
        var addr2 = new HostPort("host2", 9092);
        var forwarding = new ClientConnectionState.Forwarding();
        clientConnectionStateMachine.forceState(
                forwarding,
                frontendHandler,
                Map.of(addr1, scsm1, addr2, scsm2),
                TEST_KAFKA_SESSION,
                true,
                Map.of("route-a", addr1, "route-b", addr2));
        Object msg = new Object();

        // When
        clientConnectionStateMachine.forwardToRoute("route-b", msg);

        // Then
        verify(scsm2).sendRequest(msg);
        verifyNoInteractions(scsm1);
    }

    @Test
    void forwardToRouteShouldShareScsmForRoutesWithSameTarget() {
        // Given
        stubAsRouterVirtualCluster();
        var scsm = mock(ServerConnectionStateMachine.class);
        var addr = new HostPort("host1", 9092);
        var forwarding = new ClientConnectionState.Forwarding();
        clientConnectionStateMachine.forceState(
                forwarding,
                frontendHandler,
                Map.of(addr, scsm),
                TEST_KAFKA_SESSION,
                true,
                Map.of("route-a", addr, "route-b", addr));
        Object msg1 = new Object();
        Object msg2 = new Object();

        // When
        clientConnectionStateMachine.forwardToRoute("route-a", msg1);
        clientConnectionStateMachine.forwardToRoute("route-b", msg2);

        // Then
        verify(scsm).sendRequest(msg1);
        verify(scsm).sendRequest(msg2);
    }

    @Test
    void forwardToRouteWithUnknownRouteShouldTransitionToClosed() {
        // Given
        stubAsRouterVirtualCluster();
        var forwarding = new ClientConnectionState.Forwarding();
        clientConnectionStateMachine.forceState(
                forwarding,
                frontendHandler,
                Map.of(BROKER_ADDRESS, serverConnectionStateMachine),
                TEST_KAFKA_SESSION,
                true,
                Map.of("known-route", BROKER_ADDRESS));

        // When
        clientConnectionStateMachine.forwardToRoute("unknown-route", new Object());

        // Then
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
    }

    @Test
    void forwardToRouteWithNoScsmForTargetShouldCreateConnectionLazily() {
        // routeTargets maps a route to a target, but serverConnections has no SCSM yet.
        // forwardToRoute should create the SCSM lazily and forward the request.
        stubAsRouterVirtualCluster();
        var orphanTarget = new HostPort("orphan", 9092);
        var msg = new Object();
        clientConnectionStateMachine.forceState(
                new ClientConnectionState.Forwarding(),
                frontendHandler,
                Map.of(),
                TEST_KAFKA_SESSION,
                true,
                Map.of("route-a", orphanTarget));

        // When
        clientConnectionStateMachine.forwardToRoute("route-a", msg);

        // Then: SCSM was created and the request forwarded; state remains Forwarding
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Forwarding.class);
        verify(serverConnectionStateMachine).sendRequest(msg);
    }

    @Test
    void forwardToRouteNotInForwardingShouldTransitionToClosed() {
        // Given
        stubAsRouterVirtualCluster();
        stateMachineInClientActive();

        // When
        clientConnectionStateMachine.forwardToRoute("any-route", new Object());

        // Then
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
    }

    @Test
    void shouldFlushToClientWhenServerReadCompletes() {
        // Given
        stateMachineInForwarding();

        // When
        clientConnectionStateMachine.onServerReadComplete();

        // Then
        verify(frontendHandler).flushToClient();
    }

    @Test
    void onClientUnwritableShouldApplyBackpressureToAllServerConnections() {
        // Given
        var scsm1 = mock(ServerConnectionStateMachine.class);
        var scsm2 = mock(ServerConnectionStateMachine.class);
        var addr1 = new HostPort("host1", 9092);
        var addr2 = new HostPort("host2", 9092);
        clientConnectionStateMachine.forceState(
                new ClientConnectionState.Forwarding(),
                frontendHandler,
                Map.of(addr1, scsm1, addr2, scsm2),
                TEST_KAFKA_SESSION,
                true);

        // When
        clientConnectionStateMachine.onClientUnwritable();

        // Then
        verify(scsm1).applyBackpressure();
        verify(scsm2).applyBackpressure();
    }

    @Test
    void onClientWritableShouldRelieveBackpressureOnAllServerConnections() {
        // Given
        var scsm1 = mock(ServerConnectionStateMachine.class);
        var scsm2 = mock(ServerConnectionStateMachine.class);
        var addr1 = new HostPort("host1", 9092);
        var addr2 = new HostPort("host2", 9092);
        clientConnectionStateMachine.forceState(
                new ClientConnectionState.Forwarding(),
                frontendHandler,
                Map.of(addr1, scsm1, addr2, scsm2),
                TEST_KAFKA_SESSION,
                true);

        // When
        clientConnectionStateMachine.onClientWritable();

        // Then
        verify(scsm1).relieveBackpressure();
        verify(scsm2).relieveBackpressure();
    }

    @Test
    void shouldNotUnblockClientWhenOnlyOneBackendBecomesWritable() {
        // Given
        var scsm1 = mock(ServerConnectionStateMachine.class);
        var scsm2 = mock(ServerConnectionStateMachine.class);
        var addr1 = new HostPort("host1", 9092);
        var addr2 = new HostPort("host2", 9092);
        clientConnectionStateMachine.forceState(
                new ClientConnectionState.Forwarding(),
                frontendHandler,
                Map.of(addr1, scsm1, addr2, scsm2),
                TEST_KAFKA_SESSION,
                true);
        clientConnectionStateMachine.clientReadsBlocked = true;
        lenient().when(scsm1.isWritable()).thenReturn(true);
        when(scsm2.isWritable()).thenReturn(false);

        // When
        clientConnectionStateMachine.onServerWritable();

        // Then
        assertThat(clientConnectionStateMachine.clientReadsBlocked).isTrue();
        verify(frontendHandler, never()).relieveBackpressure();
    }

    @Test
    void shouldUnblockClientWhenAllBackendsWritable() {
        // Given
        var scsm1 = mock(ServerConnectionStateMachine.class);
        var scsm2 = mock(ServerConnectionStateMachine.class);
        var addr1 = new HostPort("host1", 9092);
        var addr2 = new HostPort("host2", 9092);
        clientConnectionStateMachine.forceState(
                new ClientConnectionState.Forwarding(),
                frontendHandler,
                Map.of(addr1, scsm1, addr2, scsm2),
                TEST_KAFKA_SESSION,
                true);
        clientConnectionStateMachine.clientReadsBlocked = true;
        lenient().when(scsm1.isWritable()).thenReturn(true);
        lenient().when(scsm2.isWritable()).thenReturn(true);

        // When
        clientConnectionStateMachine.onServerWritable();

        // Then
        assertThat(clientConnectionStateMachine.clientReadsBlocked).isFalse();
        verify(frontendHandler).relieveBackpressure();
    }

    private int getVirtualNodeClientToProxyActiveConnections() {
        return io.kroxylicious.proxy.internal.util.Metrics.clientToProxyConnectionCounter(VIRTUAL_CLUSTER_NODE).get();
    }

    private int getVirtualNodeProxyToServerActiveConnections() {
        return io.kroxylicious.proxy.internal.util.Metrics.proxyToServerConnectionCounter(VIRTUAL_CLUSTER_NODE).get();
    }

    @Test
    void onClientTlsHandshakeSuccessPassesExecutorToSubjectManager() {
        // Given
        clientConnectionStateMachine.onClientActive(frontendHandler);
        SSLSession sslSession = mock(SSLSession.class);
        AtomicBoolean executorUsed = new AtomicBoolean(false);
        when(frontendHandler.eventLoopExecutor()).thenReturn(command -> {
            executorUsed.set(true);
            command.run();
        });

        // When
        clientConnectionStateMachine.onClientTlsHandshakeSuccess(sslSession);

        // Then - verify the executor was actually used, proving the new parameter is passed through correctly
        assertThat(executorUsed).isTrue();
    }

    @org.junit.jupiter.api.Nested
    class DisconnectMetricsTest {

        @Test
        void shouldIncrementIdleTimeoutCauseOnClientIdle() {
            // Given
            stateMachineInForwarding();

            // When
            clientConnectionStateMachine.onClientIdle();

            // Then
            assertThat(Metrics.globalRegistry.find("kroxylicious_client_to_proxy_disconnects")
                    .tag("virtual_cluster", CLUSTER_NAME)
                    .tag("node_id", "bootstrap")
                    .tag("cause", "idle_timeout")
                    .counter())
                    .isNotNull()
                    .satisfies(counter -> assertThat(counter.count()).isEqualTo(1.0));
        }

        @Test
        void shouldIncrementClientClosedCauseOnClientInactive() {
            // Given
            stateMachineInForwarding();

            // When
            clientConnectionStateMachine.onClientInactive();

            // Then
            assertThat(Metrics.globalRegistry.find("kroxylicious_client_to_proxy_disconnects")
                    .tag("virtual_cluster", CLUSTER_NAME)
                    .tag("node_id", "bootstrap")
                    .tag("cause", "client_closed")
                    .counter())
                    .isNotNull()
                    .satisfies(counter -> assertThat(counter.count()).isEqualTo(1.0));
        }

        @Test
        void shouldIncrementServerClosedCauseOnServerInactive() {
            // Given
            stateMachineInForwarding();

            // When
            clientConnectionStateMachine.onServerConnectionClosed(ClientConnectionStateMachine.DisconnectCause.SERVER_CLOSED);

            // Then
            assertThat(Metrics.globalRegistry.find("kroxylicious_client_to_proxy_disconnects")
                    .tag("virtual_cluster", CLUSTER_NAME)
                    .tag("node_id", "bootstrap")
                    .tag("cause", "server_closed")
                    .counter())
                    .isNotNull()
                    .satisfies(counter -> assertThat(counter.count()).isEqualTo(1.0));
        }

        @Test
        void shouldNotDoubleCountOnSubsequentCloseAfterIdle() {
            // Given
            stateMachineInForwarding();

            // When - idle timeout followed by client inactive
            clientConnectionStateMachine.onClientIdle();
            clientConnectionStateMachine.onClientInactive();

            // Then - should only count once for idle, not again for client_closed
            assertThat(simpleMeterRegistry.counter("kroxylicious_client_to_proxy_disconnects",
                    "virtual_cluster", CLUSTER_NAME,
                    "node_id", "bootstrap",
                    "cause", "idle_timeout").count())
                    .isEqualTo(1.0);
            assertThat(simpleMeterRegistry.counter("kroxylicious_client_to_proxy_disconnects",
                    "virtual_cluster", CLUSTER_NAME,
                    "node_id", "bootstrap",
                    "cause", "client_closed").count())
                    .isEqualTo(0.0); // Should not be incremented
        }

        @Test
        void shouldNotIncrementDisconnectMetricsOnErrors() {
            // Given
            stateMachineInForwarding();

            // When - error causes disconnect
            clientConnectionStateMachine.onClientException(new RuntimeException("test error"));

            // Then - client_closed disconnect counter should not be incremented
            assertThat(simpleMeterRegistry.counter("kroxylicious_client_to_proxy_disconnects",
                    "virtual_cluster", CLUSTER_NAME,
                    "node_id", "bootstrap",
                    "cause", "client_closed").count())
                    .isEqualTo(0.0);
        }
    }

    /**
     * Focused tests for the drain branches of {@link ClientConnectionStateMachine}.
     * <p>
     * Exercises the public {@link ClientConnectionStateMachine#drain(Duration)}
     * entry point and the per-state drain branches in {@code messageFromServer},
     * {@code onClientRequest}, and {@code toClosed} that are reached only when the CCSM is
     * in {@link ClientConnectionState.Draining} state.
     * <p>
     * Inherits the outer class's mocks (frontendHandler, serverConnectionStateMachine, etc.) and adds
     * channel/event-loop/scheduled-future stubs needed to drive the drain machinery's
     * dispatching synchronously.
     */
    @Nested
    class DrainTests {

        private static final Duration DRAIN_TIMEOUT = Duration.ofSeconds(5);

        @Mock(strictness = Mock.Strictness.LENIENT)
        private Channel clientChannel;
        @Mock(strictness = Mock.Strictness.LENIENT)
        private EventLoop eventLoop;
        @Mock(strictness = Mock.Strictness.LENIENT)
        private ScheduledFuture<?> scheduledFuture;

        @BeforeEach
        void drainSetUp() {
            // Add channel + event-loop machinery on top of the outer setUp's frontendHandler stubs.
            // execute() runs the runnable synchronously so dispatched on*() methods fire in-test.
            when(frontendHandler.clientChannel()).thenReturn(clientChannel);
            when(clientChannel.eventLoop()).thenReturn(eventLoop);
            doAnswer(invocation -> {
                Runnable r = invocation.getArgument(0);
                r.run();
                return null;
            }).when(eventLoop).execute(any(Runnable.class));
            // schedule() returns a controllable future so tests can verify cancellation. The
            // timer task itself is captured via ArgumentCaptor in tests that need to fire it.
            doAnswer(invocation -> scheduledFuture).when(eventLoop)
                    .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        }

        // --- drain(Duration) entry point ---

        @Test
        void drainFromForwardingWithNoInFlightImmediatelyClosesWithDrainCompleted() {
            // Given — Forwarding state, no in-flight requests
            stateMachineInForwarding();

            // When
            CompletableFuture<Void> closedFuture = clientConnectionStateMachine.drain(DRAIN_TIMEOUT);

            // Then — the immediate-fire path runs through onDrainCompleted → toClosed,
            // so the future completes and the state ends up at Closed
            assertThat(closedFuture).isCompleted();
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
            // The drain-completed metric was incremented (proves DisconnectCause routing)
            assertThat(Metrics.globalRegistry.get("kroxylicious_client_to_proxy_disconnects")
                    .tag("cause", "drain_completed").counter().count()).isEqualTo(1.0);
            // Timer was cancelled by the onDrained policy
            verify(scheduledFuture).cancel(false);
        }

        @Test
        void drainFromForwardingWithInFlightTransitionsToDrainingAndAppliesBackpressure() {
            // Given — Forwarding with one in-flight client request (so drain has work to wait for)
            stateMachineInForwarding();
            bumpClientInFlightCount();

            // When
            CompletableFuture<Void> closedFuture = clientConnectionStateMachine.drain(DRAIN_TIMEOUT);

            // Then — state is Draining, autoRead disabled, future still pending
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Draining.class);
            assertThat(closedFuture).isNotCompleted();
            verify(frontendHandler).applyBackpressure();
            // Timer was scheduled but not yet cancelled
            verify(eventLoop).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
            verify(scheduledFuture, never()).cancel(false);
        }

        @Test
        void drainWhenStateIsNotForwardingStillCompletesFuture() {
            // Given — CCSM stuck in HaProxy state (not Forwarding)
            clientConnectionStateMachine.forceState(new ClientConnectionState.HaProxy(), frontendHandler, Map.of(), TEST_KAFKA_SESSION, true);

            // When
            CompletableFuture<Void> closedFuture = clientConnectionStateMachine.drain(DRAIN_TIMEOUT);

            // Then — the reject path in onDraining still fires the onDrained policy so DC
            // (or any caller awaiting the future) doesn't hang waiting for a drain that never starts
            assertThat(closedFuture).isCompleted();
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.HaProxy.class);
            // No autoRead change because we never entered Draining
            verify(frontendHandler, never()).applyBackpressure();
        }

        // --- messageFromServer Draining branch ---

        @Test
        void messageFromServerInDrainingFiresPolicyWhenInFlightHitsZero() {
            // Given — Forwarding with one in-flight, then drain begins (state goes Draining)
            stateMachineInForwarding();
            bumpClientInFlightCount();
            CompletableFuture<Void> closedFuture = clientConnectionStateMachine.drain(DRAIN_TIMEOUT);
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Draining.class);

            // When — server delivers a response, decrementing client-in-flight to 0
            clientConnectionStateMachine.onResponseFromServer(new Object());

            // Then — drain policy fired, state advanced to Closed (via onDrainCompleted → toClosed)
            assertThat(closedFuture).isCompleted();
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
            verify(scheduledFuture).cancel(false);
        }

        @Test
        void messageFromServerInDrainingDoesNotFirePolicyWhenInFlightStillPositive() {
            // Given — Forwarding with TWO in-flight, then drain begins
            stateMachineInForwarding();
            bumpClientInFlightCount();
            bumpClientInFlightCount();
            CompletableFuture<Void> closedFuture = clientConnectionStateMachine.drain(DRAIN_TIMEOUT);
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Draining.class);

            // When — server delivers ONE response (client-in-flight goes 2 → 1, still > 0)
            clientConnectionStateMachine.onResponseFromServer(new Object());

            // Then — still draining, future not yet completed (the "still waiting" branch ran)
            assertThat(closedFuture).isNotCompleted();
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Draining.class);
        }

        // --- onClientRequest Draining drop branch ---

        @Test
        void onClientRequestInDrainingReleasesRequestFrameAndFiresPolicyWhenCounterReachesZero() {
            // Given — drain in progress with one in-flight; the in-flight one hasn't been
            // delivered yet, so a *new* RequestFrame arriving in Draining triggers the
            // drop+compensate path
            stateMachineInForwarding();
            bumpClientInFlightCount();
            CompletableFuture<Void> closedFuture = clientConnectionStateMachine.drain(DRAIN_TIMEOUT);
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Draining.class);

            // When — a buffered request frame arrives at onClientRequest while in Draining
            var lateFrame = makeRequestFrame();
            clientConnectionStateMachine.onClientRequest(lateFrame);

            // Then — frame released; the in-flight request still pending so drain still active
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Draining.class);
            assertThat(closedFuture).isNotCompleted();

            // Now deliver the response for the original in-flight; counter goes 1 → 0 → policy fires
            clientConnectionStateMachine.onResponseFromServer(new Object());
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
            assertThat(closedFuture).isCompleted();
        }

        @Test
        void onClientRequestInDrainingReleasesNonRequestFrameWithoutTouchingCounter() {
            // Given — drain in progress with one in-flight
            stateMachineInForwarding();
            bumpClientInFlightCount();
            CompletableFuture<Void> closedFuture = clientConnectionStateMachine.drain(DRAIN_TIMEOUT);

            // When — a non-RequestFrame (e.g. a control object) arrives in Draining
            Object nonFrame = new Object();
            clientConnectionStateMachine.onClientRequest(nonFrame);

            // Then — released without compensating the counter; drain still pending
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Draining.class);
            assertThat(closedFuture).isNotCompleted();
        }

        // --- toClosed orphan-close path (channel closes mid-Draining) ---

        @Test
        void orphanCloseDuringDrainingFiresPendingDrainCallbackAndCompletesFuture() {
            // Given — drain in progress with in-flight work pending
            stateMachineInForwarding();
            bumpClientInFlightCount();
            CompletableFuture<Void> closedFuture = clientConnectionStateMachine.drain(DRAIN_TIMEOUT);
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Draining.class);
            assertThat(closedFuture).isNotCompleted();

            // When — the client connection drops mid-drain (e.g. peer disconnect → exception)
            clientConnectionStateMachine.onClientException(new RuntimeException("client gone"));

            // Then — toClosed's orphan-close path captured the pendingDrainCallback and ran it
            // on the way out, so the per-connection future still completes and the timer is
            // cancelled even though the drain didn't finish naturally
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
            assertThat(closedFuture).isCompleted();
            verify(scheduledFuture).cancel(false);
        }

        // --- onDrainTimeout — fired by the scheduled timer when drain doesn't complete in time ---

        @Test
        void onDrainTimeoutForceClosesWithDrainTimeoutCause() {
            // Given — drain in progress with in-flight work pending
            stateMachineInForwarding();
            bumpClientInFlightCount();

            // Capture the scheduled timer task so we can fire it manually
            ArgumentCaptor<Runnable> timerCaptor = ArgumentCaptor.forClass(Runnable.class);
            CompletableFuture<Void> closedFuture = clientConnectionStateMachine.drain(DRAIN_TIMEOUT);
            verify(eventLoop).schedule(timerCaptor.capture(), anyLong(), any(TimeUnit.class));
            Runnable timerTask = timerCaptor.getValue();

            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Draining.class);
            assertThat(closedFuture).isNotCompleted();

            // When — the timer fires (simulating timeout elapse)
            timerTask.run();

            // Then — state forced to Closed with DRAIN_TIMEOUT cause
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
            assertThat(closedFuture).isCompleted();
            assertThat(Metrics.globalRegistry.get("kroxylicious_client_to_proxy_disconnects")
                    .tag("cause", "drain_timeout").counter().count()).isEqualTo(1.0);
        }

        @Test
        void onDrainTimeoutWhenAlreadyClosedIsNoOp() {
            // Given — drain in progress, capture timer, then close via natural drain completion
            stateMachineInForwarding();
            bumpClientInFlightCount();
            ArgumentCaptor<Runnable> timerCaptor = ArgumentCaptor.forClass(Runnable.class);
            CompletableFuture<Void> closedFuture = clientConnectionStateMachine.drain(DRAIN_TIMEOUT);
            verify(eventLoop).schedule(timerCaptor.capture(), anyLong(), any(TimeUnit.class));
            clientConnectionStateMachine.onResponseFromServer(new Object());
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
            assertThat(closedFuture).isCompleted();
            double drainCompletedBefore = Metrics.globalRegistry.get("kroxylicious_client_to_proxy_disconnects")
                    .tag("cause", "drain_completed").counter().count();

            // When — the (now-stale) timer fires after natural completion
            timerCaptor.getValue().run();

            // Then — no-op: state stays Closed, DRAIN_COMPLETED metric unchanged
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
            assertThat(Metrics.globalRegistry.get("kroxylicious_client_to_proxy_disconnects")
                    .tag("cause", "drain_completed").counter().count()).isEqualTo(drainCompletedBefore);
        }

        // --- drain() idempotency ---

        @Test
        void secondDrainCallWhileAlreadyDrainingDoesNotScheduleNewTimer() {
            // Given — drain in progress with in-flight work pending
            stateMachineInForwarding();
            bumpClientInFlightCount();
            clientConnectionStateMachine.drain(DRAIN_TIMEOUT);

            // When — drain() called again while already draining
            CompletableFuture<Void> secondFuture = clientConnectionStateMachine.drain(DRAIN_TIMEOUT);

            // Then — only one timer scheduled in total (from the first drain call), second future pending
            verify(eventLoop, times(1)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
            assertThat(secondFuture).isNotCompleted();
        }

        @Test
        void secondDrainFutureCompletesWhenDrainCompletes() {
            // Given — drain in progress with in-flight work, second drain() already called
            stateMachineInForwarding();
            bumpClientInFlightCount();
            clientConnectionStateMachine.drain(DRAIN_TIMEOUT);
            CompletableFuture<Void> secondFuture = clientConnectionStateMachine.drain(DRAIN_TIMEOUT);
            assertThat(secondFuture).isNotCompleted();

            // When — the in-flight response arrives, completing the drain naturally
            clientConnectionStateMachine.onResponseFromServer(new Object());

            // Then — the second future completes along with the connection closing
            assertThat(secondFuture).isCompleted();
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
        }

        /**
         * Drives a request through {@code onClientRequest} while in Forwarding to bump
         * the internal client-in-flight counter by one. {@code admitToFilterChain} is
         * mocked as a no-op, so this is purely a counter-bumping action.
         */
        private void bumpClientInFlightCount() {
            if (!(clientConnectionStateMachine.state() instanceof ClientConnectionState.Forwarding)) {
                stateMachineInForwarding();
            }
            clientConnectionStateMachine.onClientRequest(makeRequestFrame());
        }

        private DecodedRequestFrame<ApiVersionsRequestData> makeRequestFrame() {
            return new DecodedRequestFrame<>(
                    ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION,
                    1,
                    false,
                    new RequestHeaderData(),
                    new ApiVersionsRequestData()
                            .setClientSoftwareName("test-client")
                            .setClientSoftwareVersion("1.0.0"));
        }
    }
}
