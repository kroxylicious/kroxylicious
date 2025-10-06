/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.net.ssl.SSLException;

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
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.ProxyChannelState.ApiVersions;
import io.kroxylicious.proxy.internal.ProxyChannelState.SelectingServer;
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.internal.util.VirtualClusterNode;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProxyChannelStateMachineTest {

    private static final HostPort BROKER_ADDRESS = new HostPort("localhost", 9092);
    private static final HAProxyMessage HA_PROXY_MESSAGE = new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
            "1.1.1.1", "2.2.2.2", 46421, 9092);
    private static final Offset<Double> CLOSE_ENOUGH = Offset.offset(0.00005);
    private static final String CLUSTER_NAME = "virtualClusterA";
    private static final VirtualClusterNode VIRTUAL_CLUSTER_NODE = new VirtualClusterNode(CLUSTER_NAME, null);
    private static final VirtualClusterModel VIRTUAL_CLUSTER_MODEL = new VirtualClusterModel(CLUSTER_NAME, new TargetCluster("", Optional.empty()), false, false,
            List.of());
    private final RuntimeException failure = new RuntimeException("There's Klingons on the starboard bow");
    private ProxyChannelStateMachine proxyChannelStateMachine;
    private KafkaProxyBackendHandler backendHandler;
    private KafkaProxyFrontendHandler frontendHandler;
    private SimpleMeterRegistry simpleMeterRegistry;

    @BeforeEach
    void setUp() {
        proxyChannelStateMachine = new ProxyChannelStateMachine(CLUSTER_NAME, null);
        backendHandler = mock(KafkaProxyBackendHandler.class);
        frontendHandler = mock(KafkaProxyFrontendHandler.class);
        simpleMeterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(simpleMeterRegistry);
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
        proxyChannelStateMachine.onClientActive(frontendHandler);

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
        proxyChannelStateMachine.onClientException(failure, tlsEnabled);

        // Then
        assertThat(Metrics.globalRegistry.get("kroxylicious_client_to_proxy_errors").counter())
                .isNotNull()
                .satisfies(counter -> assertThat(counter.getId()).isNotNull())
                .satisfies(counter -> assertThat(counter.count())
                        .isCloseTo(1.0, CLOSE_ENOUGH));
    }

    @ParameterizedTest
    @MethodSource("givenStates")
    void shouldCountProxyToServerExceptions(Runnable givenState) {
        // Given
        givenState.run();

        // When
        proxyChannelStateMachine.onServerException(failure);

        // Then
        assertThat(Metrics.globalRegistry.get("kroxylicious_proxy_to_server_errors").counter())
                .isNotNull()
                .satisfies(counter -> assertThat(counter.getId()).isNotNull())
                .satisfies(counter -> assertThat(counter.count())
                        .isCloseTo(1.0, CLOSE_ENOUGH));
    }

    @Test
    void shouldCountProxyToServerConnections() {
        // Given
        stateMachineInSelectingServer();

        // When
        proxyChannelStateMachine.onNetFilterInitiateConnect(HostPort.parse("localhost:9090"), List.of(), VIRTUAL_CLUSTER_MODEL, null);

        // Then
        assertThat(Metrics.globalRegistry.get("kroxylicious_proxy_to_server_connections").counter())
                .isNotNull()
                .satisfies(counter -> assertThat(counter.getId()).isNotNull())
                .satisfies(counter -> assertThat(counter.count())
                        .isCloseTo(1.0, CLOSE_ENOUGH));
    }

    @Test
    void shouldCountProxyToServerConnectionsFailures() {
        // Given
        stateMachineInConnecting();

        // When
        proxyChannelStateMachine.onServerException(failure);

        // Then
        assertThat(Metrics.globalRegistry.get("kroxylicious_proxy_to_server_errors").counter())
                .isNotNull()
                .satisfies(counter -> assertThat(counter.getId()).isNotNull())
                .satisfies(counter -> assertThat(counter.count())
                        .isCloseTo(1.0, CLOSE_ENOUGH));
    }

    @Test
    void shouldBlockClientReads() {
        // Given
        stateMachineInClientActive();
        proxyChannelStateMachine.onServerUnwritable();

        // When
        proxyChannelStateMachine.onServerUnwritable();

        // Then
        verify(frontendHandler, times(1)).applyBackpressure();
    }

    @Test
    void shouldUnblockClientReads() {
        // Given
        stateMachineInClientActive();
        proxyChannelStateMachine.clientReadsBlocked = true;
        proxyChannelStateMachine.onServerWritable();

        // When
        proxyChannelStateMachine.onServerWritable();

        // Then
        verify(frontendHandler, times(1)).relieveBackpressure();
    }

    @Test
    void shouldBlockServerReads() {
        // Given
        stateMachineInForwarding();
        proxyChannelStateMachine.onClientUnwritable();

        // When
        proxyChannelStateMachine.onClientUnwritable();

        // Then
        verify(backendHandler, times(1)).applyBackpressure();
    }

    @Test
    void shouldCloseOnClientRuntimeException() {
        // Given
        stateMachineInForwarding();
        RuntimeException cause = new RuntimeException("Oops!");

        // When
        proxyChannelStateMachine.onClientException(cause, true);

        // Then
        assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Closed.class);
        verify(backendHandler).inClosed();
        verify(frontendHandler).inClosed(ArgumentMatchers.notNull(UnknownServerException.class));
    }

    @Test
    void shouldCloseOnClientFrameOversizedException() {
        // Given
        stateMachineInForwarding();
        RuntimeException cause = new DecoderException(new FrameOversizedException(2, 1));

        // When
        proxyChannelStateMachine.onClientException(cause, true);

        // Then
        assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Closed.class);
        verify(backendHandler).inClosed();
        verify(frontendHandler).inClosed(ArgumentMatchers.notNull(InvalidRequestException.class));
    }

    @Test
    void shouldCloseOnServerRuntimeException() {
        // Given
        stateMachineInForwarding();
        RuntimeException cause = new RuntimeException("Oops!");

        // When
        proxyChannelStateMachine.onServerException(cause);

        // Then
        assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Closed.class);
        verify(backendHandler).inClosed();
        verify(frontendHandler).inClosed(cause);
    }

    @Test
    void shouldUnblockServerReads() {
        // Given
        stateMachineInForwarding();
        proxyChannelStateMachine.serverReadsBlocked = true;

        // When
        proxyChannelStateMachine.onClientWritable();
        proxyChannelStateMachine.onClientWritable();

        // Then
        verify(backendHandler, times(1)).relieveBackpressure();
    }

    @Test
    void shouldNotifyHandlerOnTransitionFromStartToClientActive() {
        // Given

        // When
        proxyChannelStateMachine.onClientActive(frontendHandler);

        // Then
        assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.ClientActive.class);
        verify(frontendHandler, times(1)).inClientActive();
    }

    @Test
    void inClientActiveShouldCaptureHaProxyState() {
        // Given
        stateMachineInClientActive();
        var dp = mock(SaslDecodePredicate.class);

        // When
        proxyChannelStateMachine.onClientRequest(dp, HA_PROXY_MESSAGE);

        // Then
        assertThat(proxyChannelStateMachine.state())
                .asInstanceOf(InstanceOfAssertFactories.type(ProxyChannelState.HaProxy.class))
                .extracting(ProxyChannelState.HaProxy::haProxyMessage)
                .isSameAs(HA_PROXY_MESSAGE);
        verifyNoInteractions(dp);
    }

    @Test
    void inClientActiveShouldBufferWhenOnClientMetadataRequest() {
        // Given
        stateMachineInClientActive();
        var msg = metadataRequest();
        var dp = mock(SaslDecodePredicate.class);

        // When
        proxyChannelStateMachine.onClientRequest(dp, msg);

        // Then
        assertThat(proxyChannelStateMachine.state())
                .isInstanceOf(ProxyChannelState.SelectingServer.class);
        verifyNoInteractions(dp);
        verify(frontendHandler).inSelectingServer();
        verify(frontendHandler).bufferMsg(msg);
        verifyNoMoreInteractions(frontendHandler);
    }

    @Test
    void inHaProxyShouldBufferWhenOnClientApiVersionsRequest() {
        // Given
        stateMachineInHaProxy();
        var msg = apiVersionsRequest();
        var dp = new SaslDecodePredicate(false);

        // When
        proxyChannelStateMachine.onClientRequest(dp, msg);

        // Then
        assertThat(proxyChannelStateMachine.state())
                .isInstanceOf(ProxyChannelState.SelectingServer.class);
        verify(frontendHandler).inSelectingServer();
        verify(frontendHandler).bufferMsg(msg);
        verifyNoMoreInteractions(frontendHandler);
    }

    @Test
    void inHaProxyShouldCloseOnHaProxyMsg() {
        // Given
        stateMachineInHaProxy();
        var dp = new SaslDecodePredicate(false);

        // When
        proxyChannelStateMachine.onClientRequest(dp, HA_PROXY_MESSAGE);

        // Then
        assertThat(proxyChannelStateMachine.state())
                .isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).inClosed(null);
    }

    @Test
    void inHaProxyShouldBufferWhenOnClientMetadataRequest() {
        // Given
        stateMachineInHaProxy();
        var msg = metadataRequest();
        var dp = mock(SaslDecodePredicate.class);

        // When
        proxyChannelStateMachine.onClientRequest(dp, msg);

        // Then
        assertThat(proxyChannelStateMachine.state())
                .isInstanceOf(ProxyChannelState.SelectingServer.class);
        verifyNoInteractions(dp);
        verify(frontendHandler).inSelectingServer();
        verify(frontendHandler).bufferMsg(msg);
        verifyNoMoreInteractions(frontendHandler);
    }

    @Test
    void inApiVersionsShouldCloseOnClientActive() {
        // Given
        stateMachineInApiVersionsState();

        // When
        proxyChannelStateMachine.onClientActive(frontendHandler);

        // Then
        assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).inClosed(null);
    }

    @Test
    void inApiVersionsShouldBuffer() {
        // Given
        stateMachineInApiVersionsState();
        var msg = metadataRequest();
        SaslDecodePredicate dp = mock(SaslDecodePredicate.class);

        // When
        proxyChannelStateMachine.onClientRequest(dp, msg);

        // Then
        assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.SelectingServer.class);
        verify(frontendHandler).bufferMsg(msg);
    }

    @Test
    void inApiVersionsShouldCloseOnHaProxyMessage() {
        // Given
        stateMachineInApiVersionsState();
        var dp = mock(SaslDecodePredicate.class);

        // When
        proxyChannelStateMachine.onClientRequest(dp, HA_PROXY_MESSAGE);

        // Then
        assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).inClosed(null);
        verifyNoInteractions(backendHandler);
        verifyNoInteractions(dp);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void inClientActiveShouldTransitionToApiVersionsOrSelectingServer(boolean handlingSasl) {
        // Given
        stateMachineInClientActive();
        var msg = apiVersionsRequest();

        // When
        proxyChannelStateMachine.onClientRequest(
                new SaslDecodePredicate(handlingSasl),
                msg);

        // Then
        if (handlingSasl) {
            var stateAssert = assertThat(proxyChannelStateMachine.state())
                    .asInstanceOf(InstanceOfAssertFactories.type(ApiVersions.class));
            stateAssert
                    .extracting(ApiVersions::clientSoftwareName).isEqualTo("mykafkalib");
            stateAssert
                    .extracting(ApiVersions::clientSoftwareVersion).isEqualTo("1.0.0");
        }
        else {
            var stateAssert = assertThat(proxyChannelStateMachine.state())
                    .asInstanceOf(InstanceOfAssertFactories.type(SelectingServer.class));
            stateAssert
                    .extracting(SelectingServer::clientSoftwareName).isEqualTo("mykafkalib");
            stateAssert
                    .extracting(SelectingServer::clientSoftwareVersion).isEqualTo("1.0.0");
        }
        verify(frontendHandler).bufferMsg(msg);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void inSelectingServerShouldTransitionToConnectingWhenOnNetFilterInitiateConnectCalled(boolean configureSsl) throws SSLException {
        // Given
        HostPort brokerAddress = new HostPort("localhost", 9092);
        stateMachineInSelectingServer();
        var filters = List.<FilterAndInvoker> of();
        var vc = mock(VirtualClusterModel.class);
        doReturn(configureSsl ? Optional.of(SslContextBuilder.forClient().build()) : Optional.empty()).when(vc).getUpstreamSslContext();
        var nf = mock(NetFilter.class);

        // When
        proxyChannelStateMachine.onNetFilterInitiateConnect(brokerAddress, filters, vc, nf);

        // Then
        assertThat(proxyChannelStateMachine.state())
                .isInstanceOf(ProxyChannelState.Connecting.class);
        verify(frontendHandler).inConnecting(eq(brokerAddress), eq(filters), notNull(KafkaProxyBackendHandler.class));
        assertThat(proxyChannelStateMachine).extracting("backendHandler").isNotNull();
    }

    @Test
    void inClientActiveShouldCloseWhenOnNetFilterInitiateConnectCalled() {
        // Given
        HostPort brokerAddress = new HostPort("localhost", 9092);
        stateMachineInClientActive();
        var filters = List.<FilterAndInvoker> of();
        var vc = mock(VirtualClusterModel.class);
        var nf = mock(NetFilter.class);

        // When
        proxyChannelStateMachine.onNetFilterInitiateConnect(brokerAddress, filters, vc, nf);

        // Then
        assertThat(proxyChannelStateMachine.state())
                .isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).inClosed(null);
        assertThat(proxyChannelStateMachine).extracting("backendHandler").isNull();
    }

    @Test
    void inConnectingShouldCloseWhenOnNetFilterInitiateConnect() {
        // Given
        stateMachineInConnecting();

        var filters = List.<FilterAndInvoker> of();
        var vc = mock(VirtualClusterModel.class);
        var nf = mock(NetFilter.class);

        // When
        proxyChannelStateMachine.onNetFilterInitiateConnect(BROKER_ADDRESS, filters, vc, nf);

        // Then
        assertThat(proxyChannelStateMachine.state())
                .isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).inClosed(null);
        verify(backendHandler).inClosed();
    }

    @Test
    void inConnectingShouldTransitionWhenOnServerActiveCalled() {
        // Given
        stateMachineInConnecting();

        // When
        proxyChannelStateMachine.onServerActive();

        // Then
        assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Forwarding.class);

        verify(frontendHandler).inForwarding();
        verifyNoInteractions(backendHandler);
    }

    @Test
    void inConnectingShouldBufferRequests() {
        // Given
        stateMachineInConnecting();

        // When
        DecodedRequestFrame<MetadataRequestData> msg = metadataRequest();
        proxyChannelStateMachine.onClientRequest(new SaslDecodePredicate(false), msg);

        // Then
        verify(frontendHandler).bufferMsg(msg);
        assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Connecting.class);
    }

    @Test
    void inClientActiveShouldCloseWhenOnServerActiveCalled() {
        // Given
        stateMachineInClientActive();

        // When
        proxyChannelStateMachine.onServerActive();

        // Then
        assertThat(proxyChannelStateMachine.state())
                .isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).inClosed(null);
    }

    @Test
    void inForwardingShouldForwardClientRequests() {
        // Given
        var serverCtx = mock(ChannelHandlerContext.class);
        SaslDecodePredicate dp = mock(SaslDecodePredicate.class);
        var forwarding = stateMachineInForwarding();
        var msg = metadataRequest();

        // When
        proxyChannelStateMachine.onClientRequest(dp, msg);

        // Then
        assertThat(proxyChannelStateMachine.state()).isSameAs(forwarding);
        verifyNoInteractions(frontendHandler);
        verifyNoInteractions(dp);
        verifyNoInteractions(serverCtx);
        verify(backendHandler).forwardToServer(msg);
    }

    @Test
    void inForwardingShouldForwardServerResponses() {
        // Given
        var serverCtx = mock(ChannelHandlerContext.class);
        SaslDecodePredicate dp = mock(SaslDecodePredicate.class);
        var forwarding = stateMachineInForwarding();
        var msg = metadataResponse();

        // When
        proxyChannelStateMachine.messageFromServer(msg);

        // Then
        assertThat(proxyChannelStateMachine.state()).isSameAs(forwarding);
        verify(frontendHandler).forwardToClient(msg);
        verifyNoInteractions(dp);
        verifyNoInteractions(serverCtx);
        verifyNoInteractions(backendHandler);
    }

    @Test
    void inForwardingShouldTransitionToClosedOnServerInactive() {
        // Given
        stateMachineInForwarding();
        doAnswer(invocation -> assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Closed.class)).when(frontendHandler).inClosed(null);
        doNothing().when(backendHandler).inClosed();

        // When
        proxyChannelStateMachine.onServerInactive();

        // Then
        assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).inClosed(null);
        verify(backendHandler).inClosed();
    }

    @Test
    void inForwardingShouldTransitionToClosedOnClientInactive() {
        // Given
        stateMachineInForwarding();
        doAnswer(invocation -> assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Closed.class)).when(frontendHandler).inClosed(null);

        // When
        proxyChannelStateMachine.onClientInactive();

        // Then
        assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).inClosed(null);
        verify(backendHandler).inClosed();
    }

    @Test
    void shouldNotTransitionToClosedMultipleTimes() {
        // Given
        stateMachineInClosed();

        // When
        proxyChannelStateMachine.onServerInactive();

        // Then
        verifyNoInteractions(frontendHandler, backendHandler);
    }

    @Test
    void inForwardingShouldTransitionToClosedOnServerException() {
        // Given
        stateMachineInForwarding();
        final IllegalStateException illegalStateException = new IllegalStateException("She canny take it any more, captain");
        doNothing().when(backendHandler).inClosed();

        // When
        proxyChannelStateMachine.onServerException(illegalStateException);

        // Then
        assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).inClosed(illegalStateException);
        verify(backendHandler).inClosed();
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void inForwardingShouldTransitionToClosedOnClientException(boolean tlsEnabled) {
        // Given
        stateMachineInForwarding();
        final ApiException expectedException = Errors.UNKNOWN_SERVER_ERROR.exception();
        final IllegalStateException illegalStateException = new IllegalStateException("She canny take it any more, captain");
        doAnswer(invocation -> assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Closed.class)).when(frontendHandler)
                .inClosed(expectedException);
        doNothing().when(backendHandler).inClosed();

        // When
        proxyChannelStateMachine.onClientException(illegalStateException, tlsEnabled);

        // Then
        assertThat(proxyChannelStateMachine.state()).isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).inClosed(expectedException);
        verify(backendHandler).inClosed();
    }

    @Test
    void shouldReturnStateWhenInSelectingServer() {
        // Given
        stateMachineInSelectingServer();

        // When
        final SelectingServer actualSelectingServer = proxyChannelStateMachine.enforceInSelectingServer("wibble");

        // Then
        assertThat(actualSelectingServer).isNotNull();
    }

    @ParameterizedTest
    @MethodSource("givenStates")
    void shouldThrowWhenStateWhenIsNotSelectingServer(Runnable givenState) {
        // Given
        givenState.run();

        // When
        assertThatThrownBy(() -> proxyChannelStateMachine.enforceInSelectingServer("wibble"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageEndingWith("wibble");

        // Then
    }

    @ParameterizedTest
    @MethodSource("connectedStates")
    void shouldStartServerTimerWhenClientIsUnwritable(Runnable givenState) {
        // Given
        givenState.run();

        // When
        proxyChannelStateMachine.onClientUnwritable();

        // Then
        assertThat(proxyChannelStateMachine.serverBackpressureTimer)
                .isInstanceOf(Timer.Sample.class);
    }

    @ParameterizedTest
    @MethodSource("connectedStates")
    void shouldStopServerTimerWhenClientIsWritable(Runnable givenState) {
        // Given
        givenState.run();
        proxyChannelStateMachine.onClientUnwritable();

        // When
        proxyChannelStateMachine.onClientWritable();

        // Then
        assertThat(Metrics.globalRegistry.get("kroxylicious_server_to_proxy_reads_paused").timer())
                .isInstanceOf(Timer.class)
                .satisfies(timer -> assertThat(timer.count()).isGreaterThanOrEqualTo(1)
                // Count is incremented when the timer is stopped
                );
        assertThat(proxyChannelStateMachine.serverBackpressureTimer).isNull();
    }

    @ParameterizedTest
    @MethodSource("givenStates")
    void shouldStartClientTimerWhenServerIsUnwritable(Runnable givenState) {
        // Given
        givenState.run();

        // When
        proxyChannelStateMachine.onServerUnwritable();

        // Then
        assertThat(proxyChannelStateMachine.clientToProxyBackpressureTimer)
                .isInstanceOf(Timer.Sample.class);
    }

    @ParameterizedTest
    @MethodSource("givenStates")
    void shouldStopClientTimerWhenServerIsWritable(Runnable givenState) {
        // Given
        givenState.run();
        proxyChannelStateMachine.onServerUnwritable();

        // When
        proxyChannelStateMachine.onServerWritable();

        // Then
        assertThat(Metrics.globalRegistry.get("kroxylicious_client_to_proxy_reads_paused").timer())
                .isInstanceOf(Timer.class)
                .satisfies(timer -> assertThat(timer.count()).isGreaterThanOrEqualTo(1)
                // Count is incremented when the timer is stopped
                );
        assertThat(proxyChannelStateMachine.clientToProxyBackpressureTimer).isNull();
    }

    public Stream<Arguments> clientErrorStates() {
        return Stream.of(
                argumentSet("STARTING TLS on", (Runnable) () -> {
                    // no Op
                }, true),
                argumentSet("STARTING TLS off ", (Runnable) () -> {
                    // no Op
                }, false),
                argumentSet("API Versions TLS on", (Runnable) this::stateMachineInApiVersionsState, true),
                argumentSet("API Versions TLS off ", (Runnable) this::stateMachineInApiVersionsState, false),
                argumentSet("HA Proxy TLS on", (Runnable) this::stateMachineInHaProxy, true),
                argumentSet("HA Proxy TLS off ", (Runnable) this::stateMachineInHaProxy, false),
                argumentSet("Selecting Server TLS on", (Runnable) this::stateMachineInSelectingServer, true),
                argumentSet("Selecting Server TLS off ", (Runnable) this::stateMachineInSelectingServer, false),
                argumentSet("Connecting TLS on", (Runnable) this::stateMachineInConnecting, true),
                argumentSet("Connecting TLS off ", (Runnable) this::stateMachineInConnecting, false),
                argumentSet("Client Active TLS on", (Runnable) this::stateMachineInClientActive, true),
                argumentSet("Client Active TLS off ", (Runnable) this::stateMachineInClientActive, false),
                argumentSet("Forwarding TLS on", (Runnable) this::stateMachineInForwarding, true),
                argumentSet("Forwarding TLS off ", (Runnable) this::stateMachineInForwarding, false),
                argumentSet("Closed TLS on", (Runnable) this::stateMachineInClosed, true),
                argumentSet("Closed TLS off ", (Runnable) this::stateMachineInClosed, false));
    }

    public Stream<Arguments> givenStates() {
        return Stream.of(
                argumentSet("API Versions", (Runnable) this::stateMachineInApiVersionsState),
                argumentSet("HA Proxy", (Runnable) this::stateMachineInHaProxy),
                argumentSet("Connecting", (Runnable) this::stateMachineInConnecting),
                argumentSet("ClientActive ", (Runnable) this::stateMachineInClientActive),
                argumentSet("Forwarding", (Runnable) this::stateMachineInForwarding),
                argumentSet("Closed", (Runnable) this::stateMachineInClosed));
    }

    public Stream<Arguments> connectedStates() {
        return Stream.of(
                argumentSet("Connecting", (Runnable) this::stateMachineInConnecting),
                argumentSet("Forwarding", (Runnable) this::stateMachineInForwarding),
                argumentSet("Closed", (Runnable) this::stateMachineInClosed));
    }

    private void stateMachineInClientActive() {
        proxyChannelStateMachine.forceState(
                new ProxyChannelState.ClientActive(),
                frontendHandler,
                null);
    }

    private void stateMachineInHaProxy() {
        proxyChannelStateMachine.forceState(
                new ProxyChannelState.HaProxy(HA_PROXY_MESSAGE),
                frontendHandler,
                null);
    }

    private void stateMachineInApiVersionsState() {
        proxyChannelStateMachine.forceState(
                new ProxyChannelState.ApiVersions(null, null, null),
                frontendHandler,
                null);
    }

    private void stateMachineInSelectingServer() {
        proxyChannelStateMachine.forceState(
                new ProxyChannelState.SelectingServer(null, null, null),
                frontendHandler,
                null);
    }

    private void stateMachineInConnecting() {
        proxyChannelStateMachine.forceState(
                new ProxyChannelState.Connecting(null, null, null, new HostPort("localhost", 9089)),
                frontendHandler,
                backendHandler);
    }

    private ProxyChannelState.Forwarding stateMachineInForwarding() {
        var forwarding = new ProxyChannelState.Forwarding(null, null, null);
        proxyChannelStateMachine.forceState(
                forwarding,
                frontendHandler,
                backendHandler);
        return forwarding;
    }

    private void stateMachineInClosed() {
        proxyChannelStateMachine.forceState(
                new ProxyChannelState.Closed(),
                frontendHandler,
                backendHandler);
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
        proxyChannelStateMachine.onClientActive(frontendHandler);

        // Then
        assertThat(getVirtualNodeClientToProxyActiveConnections())
                .isEqualTo(initialCount + 1);
    }

    @Test
    void shouldIncrementProxyToServerActiveConnectionsOnForwarding() {
        // Given
        stateMachineInConnecting();
        int initialCount = getVirtualNodeProxyToServerActiveConnections();

        // When
        proxyChannelStateMachine.onServerActive();

        // Then
        assertThat(getVirtualNodeProxyToServerActiveConnections())
                .isEqualTo(initialCount + 1);
    }

    @Test
    void shouldDecrementActiveConnectionsOnClosed() {
        // Given - establish both client and server connections
        proxyChannelStateMachine.onClientActive(frontendHandler);
        stateMachineInConnecting();
        proxyChannelStateMachine.onServerActive();

        int initialClientCount = getVirtualNodeClientToProxyActiveConnections();
        int initialServerCount = getVirtualNodeProxyToServerActiveConnections();

        // When
        proxyChannelStateMachine.onClientInactive();

        // Then
        assertThat(getVirtualNodeClientToProxyActiveConnections())
                .isEqualTo(initialClientCount - 1);
        assertThat(getVirtualNodeProxyToServerActiveConnections())
                .isEqualTo(initialServerCount - 1);
    }

    @Test
    void shouldDecrementActiveConnectionsOnServerInactive() {
        // Given - establish both client and server connections
        proxyChannelStateMachine.onClientActive(frontendHandler);
        stateMachineInConnecting();
        proxyChannelStateMachine.onServerActive();

        int initialClientCount = getVirtualNodeClientToProxyActiveConnections();
        int initialServerCount = getVirtualNodeProxyToServerActiveConnections();

        // When
        proxyChannelStateMachine.onServerInactive();

        // Then
        assertThat(getVirtualNodeClientToProxyActiveConnections())
                .isEqualTo(initialClientCount - 1);
        assertThat(getVirtualNodeProxyToServerActiveConnections())
                .isEqualTo(initialServerCount - 1);
    }

    @Test
    void shouldDecrementActiveConnectionsOnClientException() {
        // Given - establish client connection
        proxyChannelStateMachine.onClientActive(frontendHandler);
        int initialClientCount = getVirtualNodeClientToProxyActiveConnections();

        // When
        proxyChannelStateMachine.onClientException(new RuntimeException("test exception"), false);

        // Then
        assertThat(getVirtualNodeClientToProxyActiveConnections())
                .isEqualTo(initialClientCount - 1);
    }

    @Test
    void shouldDecrementActiveConnectionsOnServerException() {
        // Given - establish both client and server connections
        proxyChannelStateMachine.onClientActive(frontendHandler);
        stateMachineInConnecting();
        proxyChannelStateMachine.onServerActive();

        int initialClientCount = getVirtualNodeClientToProxyActiveConnections();
        int initialServerCount = getVirtualNodeProxyToServerActiveConnections();

        // When
        proxyChannelStateMachine.onServerException(new RuntimeException("test exception"));

        // Then
        assertThat(getVirtualNodeClientToProxyActiveConnections())
                .isEqualTo(initialClientCount - 1);
        assertThat(getVirtualNodeProxyToServerActiveConnections())
                .isEqualTo(initialServerCount - 1);
    }

    @Test
    void shouldOnlyDecrementClientConnectionsWhenNotInForwardingState() {
        // Given - establish client connection but not server connection
        proxyChannelStateMachine.onClientActive(frontendHandler);
        int initialClientCount = getVirtualNodeClientToProxyActiveConnections();
        int initialServerCount = getVirtualNodeProxyToServerActiveConnections();

        // When - close while not in forwarding state
        proxyChannelStateMachine.onClientInactive();

        // Then - only client connections decremented
        assertThat(getVirtualNodeClientToProxyActiveConnections())
                .isEqualTo(initialClientCount - 1);
        assertThat(getVirtualNodeProxyToServerActiveConnections())
                .isEqualTo(initialServerCount); // unchanged
    }

    private int getVirtualNodeClientToProxyActiveConnections() {
        return io.kroxylicious.proxy.internal.util.Metrics.clientToProxyConnectionCounter(VIRTUAL_CLUSTER_NODE).get();
    }

    private int getVirtualNodeProxyToServerActiveConnections() {
        return io.kroxylicious.proxy.internal.util.Metrics.proxyToServerConnectionCounter(VIRTUAL_CLUSTER_NODE).get();
    }
}
