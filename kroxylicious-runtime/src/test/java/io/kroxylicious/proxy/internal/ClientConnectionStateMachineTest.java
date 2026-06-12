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

import io.kroxylicious.proxy.config.CacheConfiguration;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.net.HaProxyContext;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClientConnectionStateMachineTest {

    private static final HostPort BROKER_ADDRESS = new HostPort("localhost", 9092);
    private static final HaProxyContext HA_PROXY_CONTEXT = new HaProxyContext("1.1.1.1", "2.2.2.2", 46421, 9092, java.util.Map.of());
    private static final Offset<Double> CLOSE_ENOUGH = Offset.offset(0.00005);
    private static final String CLUSTER_NAME = "virtualClusterA";
    private static final VirtualClusterNode VIRTUAL_CLUSTER_NODE = new VirtualClusterNode(CLUSTER_NAME, null);
    private static final VirtualClusterModel VIRTUAL_CLUSTER_MODEL = new VirtualClusterModel(CLUSTER_NAME, new TargetCluster("", Optional.empty()), false, false,
            List.of(), CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10));
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
                new KafkaSession(KafkaSessionState.ESTABLISHING)) {
            @Override
            ServerConnectionStateMachine createServerConnection(HostPort remote) {
                return serverConnectionStateMachine;
            }
        };
        when(frontendHandler.channelId()).thenReturn(DefaultChannelId.newInstance());
        when(frontendHandler.remoteHost()).thenReturn("testhost.example.com");
        when(frontendHandler.remotePort()).thenReturn(9476);
        when(frontendHandler.clientChannel()).thenReturn(mock(Channel.class));
        // Make the executor run tasks synchronously for tests
        when(frontendHandler.eventLoopExecutor()).thenReturn(Runnable::run);
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
        clientConnectionStateMachine.onServerConnectionException(serverConnectionStateMachine, failure);

        // Then — server error counting is now the SCSM's concern; CCSM just transitions to Closed
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
    }

    @Test
    void shouldCountProxyToServerConnections() {
        // Given
        stateMachineInClientActive();
        when(endpointBinding.upstreamTarget()).thenReturn(BROKER_ADDRESS);

        // When — first client request triggers SCSM creation which increments the counter
        clientConnectionStateMachine.onClientRequest(metadataRequest());

        // Then
        assertThat(Metrics.globalRegistry.get("kroxylicious_proxy_to_server_connections").counter())
                .isNotNull()
                .satisfies(counter -> assertThat(counter.getId()).isNotNull())
                .satisfies(counter -> assertThat(counter.count())
                        .isCloseTo(1.0, CLOSE_ENOUGH));
    }

    @Test
    void shouldTransitionToClosedOnServerExceptionInForwardingAwaitingBackend() {
        // Given
        stateMachineInForwardingAwaitingTransportSubject();

        // When
        clientConnectionStateMachine.onServerConnectionException(serverConnectionStateMachine, failure);

        // Then — server error counting is now the SCSM's concern; CCSM just transitions to Closed
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
    }

    @Test
    void shouldBlockClientReads() {
        // Given
        stateMachineInClientActive();
        clientConnectionStateMachine.onServerUnwritable(serverConnectionStateMachine);

        // When
        clientConnectionStateMachine.onServerUnwritable(serverConnectionStateMachine);

        // Then
        verify(frontendHandler, times(1)).applyBackpressure();
    }

    @Test
    void shouldUnblockClientReads() {
        // Given
        stateMachineInClientActive();
        clientConnectionStateMachine.clientReadsBlocked = true;
        clientConnectionStateMachine.onServerWritable(serverConnectionStateMachine);

        // When
        clientConnectionStateMachine.onServerWritable(serverConnectionStateMachine);

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
        clientConnectionStateMachine.onServerConnectionException(serverConnectionStateMachine, cause);

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
        clientConnectionStateMachine.onServerConnectionActive(serverConnectionStateMachine);

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
        clientConnectionStateMachine.onServerConnectionActive(serverConnectionStateMachine);

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
        clientConnectionStateMachine.onResponseFromServer(serverConnectionStateMachine, msg);

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
        clientConnectionStateMachine.onServerConnectionClosed(serverConnectionStateMachine, ClientConnectionStateMachine.DisconnectCause.SERVER_CLOSED);

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
        clientConnectionStateMachine.onServerConnectionClosed(serverConnectionStateMachine, ClientConnectionStateMachine.DisconnectCause.SERVER_CLOSED);

        // Then
        verifyNoInteractions(frontendHandler, serverConnectionStateMachine);
    }

    @Test
    void inForwardingShouldTransitionToClosedOnServerException() {
        // Given
        stateMachineInForwarding();
        final IllegalStateException illegalStateException = new IllegalStateException("She canny take it any more, captain");
        doNothing().when(serverConnectionStateMachine).close();

        // When
        clientConnectionStateMachine.onServerConnectionException(serverConnectionStateMachine, illegalStateException);

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
        clientConnectionStateMachine.onServerUnwritable(serverConnectionStateMachine);

        // Then
        assertThat(clientConnectionStateMachine.clientToProxyBackpressureTimer)
                .isInstanceOf(Timer.Sample.class);
    }

    @ParameterizedTest
    @MethodSource("givenStates")
    void shouldStopClientTimerWhenServerIsWritable(Runnable givenState) {
        // Given
        givenState.run();
        clientConnectionStateMachine.onServerUnwritable(serverConnectionStateMachine);

        // When
        clientConnectionStateMachine.onServerWritable(serverConnectionStateMachine);

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
        clientConnectionStateMachine.onServerConnectionActive(serverConnectionStateMachine);

        // Then — state is still Forwarding (latch decremented but not zero)
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Forwarding.class);
    }

    @Test
    void shouldDecrementActiveConnectionsOnClosed() {
        // Given - establish both client and server connections
        clientConnectionStateMachine.onClientActive(frontendHandler);
        stateMachineInForwardingAwaitingTransportSubject();
        clientConnectionStateMachine.onServerConnectionActive(serverConnectionStateMachine);

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
        clientConnectionStateMachine.onServerConnectionActive(serverConnectionStateMachine);

        int initialClientCount = getVirtualNodeClientToProxyActiveConnections();

        // When
        clientConnectionStateMachine.onServerConnectionClosed(serverConnectionStateMachine, ClientConnectionStateMachine.DisconnectCause.SERVER_CLOSED);

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
        clientConnectionStateMachine.onServerConnectionActive(serverConnectionStateMachine);

        int initialClientCount = getVirtualNodeClientToProxyActiveConnections();

        // When
        clientConnectionStateMachine.onServerConnectionException(serverConnectionStateMachine, new RuntimeException("test exception"));

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
        clientConnectionStateMachine.onClientFilterChainComplete(msg);

        // Then
        verify(serverConnectionStateMachine).sendRequest(msg);
    }

    @Test
    void onClientFilterChainCompleteNotInForwarding() {
        // Given
        stateMachineInClientActive();
        Object msg = new Object();

        // When
        clientConnectionStateMachine.onClientFilterChainComplete(msg);

        // Then
        assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
        verify(frontendHandler).inClosed(null);
    }

    @Test
    void forwardToRouteShouldDispatchToCorrectScsm() {
        // Given
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
    void forwardToRouteNotInForwardingShouldTransitionToClosed() {
        // Given
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
        clientConnectionStateMachine.onServerReadComplete(serverConnectionStateMachine);

        // Then
        verify(frontendHandler).flushToClient();
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
            clientConnectionStateMachine.onServerConnectionClosed(serverConnectionStateMachine, ClientConnectionStateMachine.DisconnectCause.SERVER_CLOSED);

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
            // The drain-completed metric was incremented (proves DisconnectCause router)
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
            clientConnectionStateMachine.onResponseFromServer(serverConnectionStateMachine, new Object());

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
            clientConnectionStateMachine.onResponseFromServer(serverConnectionStateMachine, new Object());

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
            clientConnectionStateMachine.onResponseFromServer(serverConnectionStateMachine, new Object());
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
            clientConnectionStateMachine.onResponseFromServer(serverConnectionStateMachine, new Object());
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
            clientConnectionStateMachine.onResponseFromServer(serverConnectionStateMachine, new Object());

            // Then — the second future completes along with the connection closing
            assertThat(secondFuture).isCompleted();
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
        }

        // --- routing terminal handler interaction with in-flight count ---

        @Test
        void dynamicRoutingResponseDoesNotDecrementInFlightCount() {
            // Given — Forwarding with one in-flight, terminal handler active, drain started
            stateMachineInForwarding();
            var terminalHandler = mock(io.kroxylicious.proxy.internal.routing.RoutingTerminalHandler.class);
            clientConnectionStateMachine.setRoutingTerminalHandler(terminalHandler);
            bumpClientInFlightCount();
            CompletableFuture<Void> closedFuture = clientConnectionStateMachine.drain(DRAIN_TIMEOUT);
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Draining.class);

            // When — server delivers a dynamic routing response (negative correlation ID)
            var dynamicResponse = new io.kroxylicious.proxy.frame.DecodedResponseFrame<>(
                    (short) 12, -1,
                    new org.apache.kafka.common.message.ResponseHeaderData(),
                    new org.apache.kafka.common.message.FetchResponseData());
            clientConnectionStateMachine.onResponseFromServer(serverConnectionStateMachine, dynamicResponse);

            // Then — drain has NOT fired because dynamic responses are decremented by onRoutedRequestComplete
            assertThat(closedFuture).isNotCompleted();
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Draining.class);
        }

        @Test
        void onRoutedRequestCompleteDecrementsInFlightAndFiresDrain() {
            // Given — Forwarding with one in-flight, terminal handler active, drain started
            stateMachineInForwarding();
            var terminalHandler = mock(io.kroxylicious.proxy.internal.routing.RoutingTerminalHandler.class);
            clientConnectionStateMachine.setRoutingTerminalHandler(terminalHandler);
            bumpClientInFlightCount();
            CompletableFuture<Void> closedFuture = clientConnectionStateMachine.drain(DRAIN_TIMEOUT);
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Draining.class);

            // Simulate dynamic routing response (no decrement)
            var dynamicResponse = new io.kroxylicious.proxy.frame.DecodedResponseFrame<>(
                    (short) 12, -1,
                    new org.apache.kafka.common.message.ResponseHeaderData(),
                    new org.apache.kafka.common.message.FetchResponseData());
            clientConnectionStateMachine.onResponseFromServer(serverConnectionStateMachine, dynamicResponse);
            assertThat(closedFuture).isNotCompleted();

            // When — router signals the logical client request is complete
            clientConnectionStateMachine.onRoutedRequestComplete();

            // Then — in-flight count hits zero, drain fires
            assertThat(closedFuture).isCompleted();
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Closed.class);
            verify(scheduledFuture).cancel(false);
        }

        @Test
        void fanOutWithMultipleResponsesDoesNotDrainPrematurely() {
            // Given — one client request fans out to 2 backend requests
            stateMachineInForwarding();
            var terminalHandler = mock(io.kroxylicious.proxy.internal.routing.RoutingTerminalHandler.class);
            clientConnectionStateMachine.setRoutingTerminalHandler(terminalHandler);
            bumpClientInFlightCount();
            CompletableFuture<Void> closedFuture = clientConnectionStateMachine.drain(DRAIN_TIMEOUT);

            // When — two dynamic routing responses arrive (not decremented)
            var resp1 = new io.kroxylicious.proxy.frame.DecodedResponseFrame<>(
                    (short) 12, -1,
                    new org.apache.kafka.common.message.ResponseHeaderData(),
                    new org.apache.kafka.common.message.FetchResponseData());
            var resp2 = new io.kroxylicious.proxy.frame.DecodedResponseFrame<>(
                    (short) 12, -2,
                    new org.apache.kafka.common.message.ResponseHeaderData(),
                    new org.apache.kafka.common.message.FetchResponseData());
            clientConnectionStateMachine.onResponseFromServer(serverConnectionStateMachine, resp1);
            clientConnectionStateMachine.onResponseFromServer(serverConnectionStateMachine, resp2);

            // Then — still draining, in-flight count has not been decremented
            assertThat(closedFuture).isNotCompleted();
            assertThat(clientConnectionStateMachine.state()).isInstanceOf(ClientConnectionState.Draining.class);

            // When — router signals logical completion
            clientConnectionStateMachine.onRoutedRequestComplete();

            // Then — drain fires
            assertThat(closedFuture).isCompleted();
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

    @Nested
    class PerBrokerRoutingTest {

        private static final HostPort CLUSTER_A_BOOTSTRAP = new HostPort("cluster-a", 9092);
        private static final HostPort CLUSTER_B_BOOTSTRAP = new HostPort("cluster-b", 9092);
        private static final HostPort CLUSTER_A_BROKER_1 = new HostPort("cluster-a-broker1", 9092);

        private final Map<HostPort, ServerConnectionStateMachine> createdConnections = new java.util.LinkedHashMap<>();

        private ClientConnectionStateMachine routingCcsm;

        @BeforeEach
        void setUpRouting() {
            var routeA = new RouteDescriptor("route-a", 0,
                    new TargetCluster("cluster-a:9092", Optional.empty()), null, List.of());
            var routeB = new RouteDescriptor("route-b", 1,
                    new TargetCluster("cluster-b:9092", Optional.empty()), null, List.of());
            var routeDescriptors = Map.of("route-a", routeA, "route-b", routeB);

            var routingVcm = new VirtualClusterModel(CLUSTER_NAME, null, false, false,
                    List.of(), CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null,
                    "topic-router", routeDescriptors);

            var brokerBinding = new io.kroxylicious.proxy.internal.net.BrokerEndpointBinding(
                    endpointGateway, CLUSTER_A_BROKER_1, 2);

            when(endpointGateway.virtualCluster()).thenReturn(routingVcm);

            createdConnections.clear();
            routingCcsm = new ClientConnectionStateMachine(brokerBinding,
                    new DefaultSubjectBuilder(List.of()),
                    new KafkaSession(KafkaSessionState.ESTABLISHING)) {
                @Override
                ServerConnectionStateMachine createServerConnection(HostPort remote) {
                    var scsm = mock(ServerConnectionStateMachine.class);
                    createdConnections.put(remote, scsm);
                    return scsm;
                }
            };
            routingCcsm.setNodeIdMapping(
                    new io.kroxylicious.proxy.internal.routing.BijectiveNodeIdMapping(
                            Map.of("route-a", 0, "route-b", 1), 2));
        }

        @Test
        void shouldConnectOwningRouteToSpecificBroker() {
            // Given — client connected to virtual node 2 (route-a, target broker 1)
            routingCcsm.onClientActive(frontendHandler);
            routingCcsm.onClientRequest(metadataRequest());

            // When — forwardToRoute lazily opens connections
            routingCcsm.forwardToRoute("route-a", new Object());
            routingCcsm.forwardToRoute("route-b", new Object());

            // Then — route-a connects to the specific broker, route-b to its bootstrap
            assertThat(createdConnections).containsKey(CLUSTER_A_BROKER_1);
            assertThat(createdConnections).containsKey(CLUSTER_B_BOOTSTRAP);
            assertThat(createdConnections).doesNotContainKey(CLUSTER_A_BOOTSTRAP);
        }

        @Test
        void shouldUseBootstrapsWhenNoNodeIdMapping() {
            // Given — no NodeIdMapping set
            routingCcsm.setNodeIdMapping(null);
            routingCcsm.onClientActive(frontendHandler);
            routingCcsm.onClientRequest(metadataRequest());

            // When — forwardToRoute lazily opens connections
            routingCcsm.forwardToRoute("route-a", new Object());
            routingCcsm.forwardToRoute("route-b", new Object());

            // Then — both routes use their bootstraps
            assertThat(createdConnections).containsKey(CLUSTER_A_BOOTSTRAP);
            assertThat(createdConnections).containsKey(CLUSTER_B_BOOTSTRAP);
            assertThat(createdConnections).doesNotContainKey(CLUSTER_A_BROKER_1);
        }

        @Test
        void shouldForwardToOwningRouteViaSpecificBroker() {
            // Given
            routingCcsm.onClientActive(frontendHandler);
            routingCcsm.onClientRequest(metadataRequest());

            // When
            var msg = new Object();
            routingCcsm.forwardToRoute("route-a", msg);

            // Then — message goes to the SCSM for the specific broker
            var owningRouteScsm = createdConnections.get(CLUSTER_A_BROKER_1);
            verify(owningRouteScsm).sendRequest(msg);
        }

        @Test
        void forwardToNodeShouldResolveAndDispatch() {
            var targetBroker = new HostPort("cluster-a-broker2", 9092);
            routingCcsm.setUpstreamAddressResolver(
                    nodeId -> nodeId == 4 ? Optional.of(targetBroker) : Optional.empty());
            routingCcsm.onClientActive(frontendHandler);
            routingCcsm.onClientRequest(metadataRequest());

            var msg = new Object();
            // virtual node 4 = route-a, target broker 2
            routingCcsm.forwardToNode(4, "route-a", msg);

            assertThat(createdConnections).containsKey(targetBroker);
            verify(createdConnections.get(targetBroker)).sendRequest(msg);
        }

        @Test
        void forwardToNodeShouldReuseExistingConnection() {
            routingCcsm.setUpstreamAddressResolver(
                    nodeId -> nodeId == 2 ? Optional.of(CLUSTER_A_BROKER_1) : Optional.empty());
            routingCcsm.onClientActive(frontendHandler);
            routingCcsm.onClientRequest(metadataRequest());

            // Lazily create the connection for route-a (resolves to CLUSTER_A_BROKER_1)
            routingCcsm.forwardToRoute("route-a", new Object());
            int connectionsBefore = createdConnections.size();

            var msg = new Object();
            // virtual node 2 maps to CLUSTER_A_BROKER_1 which already has a connection
            routingCcsm.forwardToNode(2, "route-a", msg);

            assertThat(createdConnections).hasSize(connectionsBefore);
            verify(createdConnections.get(CLUSTER_A_BROKER_1)).sendRequest(msg);
        }

        @Test
        void forwardToNodeShouldThrowWhenAddressUnknown() {
            routingCcsm.setUpstreamAddressResolver(nodeId -> Optional.empty());
            routingCcsm.onClientActive(frontendHandler);
            routingCcsm.onClientRequest(metadataRequest());

            assertThatThrownBy(() -> routingCcsm.forwardToNode(99, "route-a", new Object()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Upstream address not yet known");
        }

        @Test
        void forwardToNodeShouldThrowWhenResolverNotSet() {
            routingCcsm.onClientActive(frontendHandler);
            routingCcsm.onClientRequest(metadataRequest());

            assertThatThrownBy(() -> routingCcsm.forwardToNode(0, "route-a", new Object()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("No upstream address resolver");
        }
    }
}
