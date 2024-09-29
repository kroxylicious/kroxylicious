/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.List;
import java.util.Optional;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.ProxyChannelState.ApiVersions;
import io.kroxylicious.proxy.internal.ProxyChannelState.SelectingServer;
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class StateHolderTest {

    public static final HostPort BROKER_ADDRESS = new HostPort("localhost", 9092);
    public static final HAProxyMessage HA_PROXY_MESSAGE = new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
            "1.1.1.1", "2.2.2.2", 46421, 9092);
    private StateHolder stateHolder;
    private KafkaProxyBackendHandler backendHandler;
    private KafkaProxyFrontendHandler frontendHandler;

    @BeforeEach
    void setUp() {
        stateHolder = new StateHolder();
        backendHandler = mock(KafkaProxyBackendHandler.class);
        frontendHandler = mock(KafkaProxyFrontendHandler.class);
    }

    private void stateHolderInClientActive() {
        stateHolder.state = new ProxyChannelState.ClientActive();
        stateHolder.backendHandler = null;
        stateHolder.frontendHandler = frontendHandler;
    }

    private void stateHolderInHaProxy() {
        stateHolder.state = new ProxyChannelState.HaProxy(HA_PROXY_MESSAGE);
        stateHolder.backendHandler = null;
        stateHolder.frontendHandler = frontendHandler;
    }

    private void stateHolderInApiVersionsState() {
        stateHolder.state = new ProxyChannelState.ApiVersions(null, null, null);
        stateHolder.frontendHandler = frontendHandler;
    }

    private void stateHolderInSelectingServer() {
        stateHolder.state = new ProxyChannelState.SelectingServer(null, null, null);
        stateHolder.backendHandler = null;
        stateHolder.frontendHandler = frontendHandler;
    }

    private ProxyChannelState.Connecting stateHolderInConnecting() {
        ProxyChannelState.Connecting state = new ProxyChannelState.Connecting(null, null, null, BROKER_ADDRESS);
        stateHolder.state = state;
        stateHolder.backendHandler = backendHandler;
        stateHolder.frontendHandler = frontendHandler;
        return state;
    }

    private ProxyChannelState.NegotiatingTls stateHolderInNegotiatingTls(ChannelHandlerContext serverCtx) {
        ProxyChannelState.NegotiatingTls state = new ProxyChannelState.NegotiatingTls(null, null, null, serverCtx, BROKER_ADDRESS);
        stateHolder.state = state;
        stateHolder.backendHandler = backendHandler;
        stateHolder.frontendHandler = frontendHandler;
        return state;
    }

    private ProxyChannelState.Forwarding stateHolderInForwarding(
            ChannelHandlerContext serverCtx) {
        var forwarding = new ProxyChannelState.Forwarding(null, null, null, serverCtx, BROKER_ADDRESS);
        stateHolder.state = forwarding;
        stateHolder.backendHandler = backendHandler;
        stateHolder.frontendHandler = frontendHandler;
        return forwarding;
    }

    @NonNull
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

    @NonNull
    private static DecodedRequestFrame<MetadataRequestData> metadataRequest() {
        return new DecodedRequestFrame<>(
                MetadataRequestData.HIGHEST_SUPPORTED_VERSION,
                0,
                false,
                new RequestHeaderData(),
                new MetadataRequestData()
        );
    }

    @NonNull
    private static DecodedResponseFrame<MetadataResponseData> metadataResponse() {
        return new DecodedResponseFrame<>(
                MetadataRequestData.HIGHEST_SUPPORTED_VERSION,
                0,
                new ResponseHeaderData(),
                new MetadataResponseData()
        );
    }

    @Test
    void shouldBlockClientReads() {
        // Given
        stateHolder.frontendHandler = frontendHandler;

        // When
        stateHolder.onServerUnwritable();
        stateHolder.onServerUnwritable();

        // Then
        verify(frontendHandler, times(1)).blockClientReads();
    }

    @Test
    void shouldUnblockClientReads() {
        // Given
        stateHolder.frontendHandler = frontendHandler;
        stateHolder.clientReadsBlocked = true;

        // When
        stateHolder.onServerWritable();
        stateHolder.onServerWritable();

        // Then
        verify(frontendHandler, times(1)).unblockClientReads();
    }

    @Test
    void shouldBlockServerReads() {
        // Given
        stateHolder.backendHandler = backendHandler;

        // When
        stateHolder.onClientUnwritable();
        stateHolder.onClientUnwritable();

        // Then
        verify(backendHandler, times(1)).blockServerReads();
    }

    @Test
    void shouldCloseOnClientRuntimeException() {
        // Given
        stateHolder.frontendHandler = frontendHandler;
        stateHolder.backendHandler = backendHandler;
        RuntimeException cause = new RuntimeException("Oops!");

        // When
        stateHolder.onClientException(cause, true);

        // Then
        assertThat(stateHolder.state).isInstanceOf(ProxyChannelState.Closed.class);
        verify(backendHandler).close();
        verify(frontendHandler).closeWithResponse(ArgumentMatchers.notNull(UnknownServerException.class));
    }

    @Test
    void shouldCloseOnClientFrameOversizedException() {
        // Given
        stateHolder.frontendHandler = frontendHandler;
        stateHolder.backendHandler = backendHandler;
        RuntimeException cause = new DecoderException(new FrameOversizedException(2, 1));

        // When
        stateHolder.onClientException(cause, true);

        // Then
        assertThat(stateHolder.state).isInstanceOf(ProxyChannelState.Closed.class);
        verify(backendHandler).close();
        verify(frontendHandler).closeWithResponse(ArgumentMatchers.notNull(InvalidRequestException.class));
    }


    @Test
    void shouldCloseOnServerRuntimeException() {
        // Given
        stateHolder.frontendHandler = frontendHandler;
        stateHolder.backendHandler = backendHandler;
        RuntimeException cause = new RuntimeException("Oops!");

        // When
        stateHolder.onServerException(cause);

        // Then
        assertThat(stateHolder.state).isInstanceOf(ProxyChannelState.Closed.class);
        verify(backendHandler).close();
        verify(frontendHandler).closeWithResponse(cause);
    }

    @Test
    void shouldUnblockServerReads() {
        // Given
        stateHolder.backendHandler = backendHandler;
        stateHolder.serverReadsBlocked = true;

        // When
        stateHolder.onClientWritable();
        stateHolder.onClientWritable();

        // Then
        verify(backendHandler, times(1)).unblockServerReads();
    }

    @Test
    void inStartStateShouldCallInClientActive() {
        // Given

        // When
        stateHolder.onClientActive(frontendHandler);

        // Then
        assertThat(stateHolder.state).isInstanceOf(ProxyChannelState.ClientActive.class);
        verify(frontendHandler, times(1)).inClientActive();
    }

    @Test
    void inClientActiveShouldCaptureHaProxyState() {
        // Given
        stateHolderInClientActive();
        var dp = mock(SaslDecodePredicate.class);

        // When
        stateHolder.onClientRequest(dp, HA_PROXY_MESSAGE);

        // Then
        assertThat(stateHolder.state)
                .asInstanceOf(InstanceOfAssertFactories.type(ProxyChannelState.HaProxy.class))
                .extracting(ProxyChannelState.HaProxy::haProxyMessage)
                .isSameAs(HA_PROXY_MESSAGE);
        verifyNoInteractions(dp);
    }

    @Test
    void inClientActiveShouldBufferWhenOnClientMetadataRequest() {
        // Given
        stateHolderInClientActive();
        var msg = metadataRequest();
        var dp = mock(SaslDecodePredicate.class);

        // When
        stateHolder.onClientRequest(dp, msg);

        // Then
        assertThat(stateHolder.state)
                .isInstanceOf(ProxyChannelState.SelectingServer.class);
        verifyNoInteractions(dp);
        verify(frontendHandler).inSelectingServer();
        verify(frontendHandler).bufferMsg(msg);
        verifyNoMoreInteractions(frontendHandler);
    }

    @Test
    void inHaProxyShouldBufferWhenOnClientApiVersionsRequest() {
        // Given
        stateHolderInHaProxy();
        var msg = apiVersionsRequest();
        var dp = new SaslDecodePredicate(false);

        // When
        stateHolder.onClientRequest(dp, msg);

        // Then
        assertThat(stateHolder.state)
                .isInstanceOf(ProxyChannelState.SelectingServer.class)
                ;
        verify(frontendHandler).inSelectingServer();
        verify(frontendHandler).bufferMsg(msg);
        verifyNoMoreInteractions(frontendHandler);
    }

    @Test
    void inHaProxyShouldCloseOnHaProxyMsg() {
        // Given
        stateHolderInHaProxy();
        var dp = new SaslDecodePredicate(false);

        // When
        stateHolder.onClientRequest(dp, HA_PROXY_MESSAGE);

        // Then
        assertThat(stateHolder.state)
                .isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).closeWithResponse(null);
    }

    @Test
    void inHaProxyShouldBufferWhenOnClientMetadataRequest() {
        // Given
        stateHolderInHaProxy();
        var msg = metadataRequest();
        var dp = mock(SaslDecodePredicate.class);

        // When
        stateHolder.onClientRequest(dp, msg);

        // Then
        assertThat(stateHolder.state)
                .isInstanceOf(ProxyChannelState.SelectingServer.class)
                ;
        verifyNoInteractions(dp);
        verify(frontendHandler).inSelectingServer();
        verify(frontendHandler).bufferMsg(msg);
        verifyNoMoreInteractions(frontendHandler);
    }

    @Test
    void inApiVersionsShouldCloseOnClientActive() {
        // Given
        stateHolderInApiVersionsState();

        // When
        stateHolder.onClientActive(frontendHandler);

        // Then
        assertThat(stateHolder.state).isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).closeWithResponse(null);
    }

    @Test
    void inApiVersionsShouldBuffer() {
        // Given
        stateHolderInApiVersionsState();
        var msg = metadataRequest();
        SaslDecodePredicate dp = mock(SaslDecodePredicate.class);

        // When
        stateHolder.onClientRequest(dp, msg);

        // Then
        assertThat(stateHolder.state).isInstanceOf(ProxyChannelState.SelectingServer.class);
        verify(frontendHandler).bufferMsg(msg);
    }

    @Test
    void inApiVersionsShouldCloseOnHaProxyMessage() {
        // Given
        stateHolderInApiVersionsState();
        var dp = mock(SaslDecodePredicate.class);

        // When
        stateHolder.onClientRequest(dp, HA_PROXY_MESSAGE);

        // Then
        assertThat(stateHolder.state).isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).closeWithResponse(null);
        verifyNoInteractions(backendHandler);
        verifyNoInteractions(dp);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void inClientActiveShouldTransitionToApiVersionsOrSelectingServer(boolean handlingSasl) {
        // Given
        stateHolderInClientActive();
        var msg = apiVersionsRequest();

        // When
        stateHolder.onClientRequest(
                new SaslDecodePredicate(handlingSasl),
                msg);

        // Then
        if (handlingSasl) {
            var stateAssert = assertThat(stateHolder.state)
                    .asInstanceOf(InstanceOfAssertFactories.type(ApiVersions.class));
            stateAssert
                    .extracting(ApiVersions::clientSoftwareName).isEqualTo("mykafkalib");
            stateAssert
                    .extracting(ApiVersions::clientSoftwareVersion).isEqualTo("1.0.0");
        }
        else {
            var stateAssert = assertThat(stateHolder.state)
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
        stateHolderInSelectingServer();
        var filters = List.<FilterAndInvoker> of();
        var vc = mock(VirtualCluster.class);
        doReturn(configureSsl ? Optional.of(SslContextBuilder.forClient().build()) : Optional.empty()).when(vc).getUpstreamSslContext();
        var nf = mock(NetFilter.class);

        // When
        stateHolder.onNetFilterInitiateConnect(brokerAddress, filters, vc, nf);

        // Then
        assertThat(stateHolder.state)
                .asInstanceOf(InstanceOfAssertFactories.type(ProxyChannelState.Connecting.class))
                .extracting(ProxyChannelState.Connecting::remote).isEqualTo(brokerAddress);
        verify(frontendHandler).inConnecting(eq(brokerAddress), eq(filters), notNull(KafkaProxyBackendHandler.class));
        assertThat(stateHolder.backendHandler).isNotNull();
    }

    @Test
    void inClientActiveShouldCloseWhenOnNetFilterInitiateConnectCalled() {
        // Given
        HostPort brokerAddress = new HostPort("localhost", 9092);
        stateHolderInClientActive();
        var filters = List.<FilterAndInvoker> of();
        var vc = mock(VirtualCluster.class);
        var nf = mock(NetFilter.class);

        // When
        stateHolder.onNetFilterInitiateConnect(brokerAddress, filters, vc, nf);

        // Then
        assertThat(stateHolder.state)
                .isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).closeWithResponse(null);
        assertThat(stateHolder.backendHandler).isNull();
    }

    @Test
    void inConnectingShouldCloseWhenOnNetFilterInitiateConnect() {
        // Given
        stateHolderInConnecting();

        var filters = List.<FilterAndInvoker> of();
        var vc = mock(VirtualCluster.class);
        var nf = mock(NetFilter.class);

        // When
        stateHolder.onNetFilterInitiateConnect(BROKER_ADDRESS, filters, vc, nf);

        // Then
        assertThat(stateHolder.state)
                .isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).closeWithResponse(null);
        verify(backendHandler).close();
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void inConnectingShouldTransitionWhenOnServerActiveCalled(boolean configureTls) throws SSLException {
        // Given
        stateHolderInConnecting();
        var serverCtx = mock(ChannelHandlerContext.class);
        SslContext sslContext;
        if (configureTls) {
            sslContext = SslContextBuilder.forClient().build();
        }
        else {
            sslContext = null;
        }

        // When
        stateHolder.onServerActive(serverCtx, sslContext);

        // Then
        if (configureTls) {
            var stateAssert = assertThat(stateHolder.state)
                    .asInstanceOf(InstanceOfAssertFactories.type(ProxyChannelState.NegotiatingTls.class));
            stateAssert
                    .extracting(ProxyChannelState.NegotiatingTls::remote).isEqualTo(BROKER_ADDRESS);
            verifyNoInteractions(frontendHandler);
            verifyNoInteractions(backendHandler);
        }
        else {
            assertThat(stateHolder.state).isInstanceOf(ProxyChannelState.Forwarding.class);

            verify(frontendHandler).inForwarding();
            verifyNoInteractions(backendHandler);
        }
    }

    @Test
    void inConnectingShouldBufferRequests() {
        // Given
        var state = stateHolderInConnecting();

        // When
        DecodedRequestFrame<MetadataRequestData> msg = metadataRequest();
        stateHolder.onClientRequest(new SaslDecodePredicate(false), msg);

        // Then
        verify(frontendHandler).bufferMsg(msg);
        assertThat(stateHolder.state).isEqualTo(state);
    }

    @Test
    void inClientActiveShouldCloseWhenOnServerActiveCalled() {
        // Given
        stateHolderInClientActive();
        var serverCtx = mock(ChannelHandlerContext.class);
        SslContext sslContext = null;

        // When
        stateHolder.onServerActive(serverCtx, sslContext);

        // Then
        assertThat(stateHolder.state)
                .isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).closeWithResponse(null);
    }

    @Test
    void inNegotiatingTlsShouldTransitionWhenOnServerTlsSuccess() {
        // Given
        var serverCtx = mock(ChannelHandlerContext.class);
        stateHolderInNegotiatingTls(serverCtx);

        // When
        stateHolder.onServerTlsHandshakeCompletion(SslHandshakeCompletionEvent.SUCCESS);

        // Then
        assertThat(stateHolder.state).isInstanceOf(ProxyChannelState.Forwarding.class);
        verify(frontendHandler).inForwarding();
        verifyNoInteractions(backendHandler);
    }

    @Test
    void inNegotiatingTlsShouldCloseWhenOnServerTlsFail() {
        // Given
        var serverCtx = mock(ChannelHandlerContext.class);
        stateHolderInNegotiatingTls(serverCtx);
        SSLHandshakeException cause = new SSLHandshakeException("Oops!");

        // When
        stateHolder.onServerTlsHandshakeCompletion(new SslHandshakeCompletionEvent(cause));

        // Then
        assertThat(stateHolder.state)
                .isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).closeWithResponse(cause);
        verify(backendHandler).close();
    }

    @Test
    void inNegotiatingTlsShouldBufferRequests() {
        // Given
        var serverCtx = mock(ChannelHandlerContext.class);
        var state = stateHolderInNegotiatingTls(serverCtx);

        // When
        DecodedRequestFrame<MetadataRequestData> msg = metadataRequest();
        stateHolder.onClientRequest(new SaslDecodePredicate(false), msg);

        // Then
        verify(frontendHandler).bufferMsg(msg);
        assertThat(stateHolder.state).isEqualTo(state);
    }


    @Test
    void inForwardingShouldForwardClientRequests() {
        // Given
        var serverCtx = mock(ChannelHandlerContext.class);
        SaslDecodePredicate dp = mock(SaslDecodePredicate.class);
        var forwarding = stateHolderInForwarding(serverCtx);
        var msg = metadataRequest();

        // When
        stateHolder.onClientRequest(dp, msg);

        // Then
        assertThat(stateHolder.state).isSameAs(forwarding);
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
        var forwarding = stateHolderInForwarding(serverCtx);
        var msg = metadataResponse();

        // When
        stateHolder.forwardToClient(msg);

        // Then
        assertThat(stateHolder.state).isSameAs(forwarding);
        verify(frontendHandler).forwardToClient(msg);
        verifyNoInteractions(dp);
        verifyNoInteractions(serverCtx);
        verifyNoInteractions(backendHandler);
    }

    @Test
    void inForwardingShouldTransitionToClosedOnServerInactive() {
        // Given
        var serverCtx = mock(ChannelHandlerContext.class);
        stateHolderInForwarding(serverCtx);

        // When
        stateHolder.onServerInactive();

        // Then
        assertThat(stateHolder.state).isInstanceOf(ProxyChannelState.Closed.class);
        verify(backendHandler).close();
        verify(frontendHandler).closeWithResponse(null);
    }

    @Test
    void inForwardingShouldTransitionToClosedOnClientInactive() {
        // Given
        var serverCtx = mock(ChannelHandlerContext.class);
        stateHolderInForwarding(serverCtx);

        // When
        stateHolder.onClientInactive();

        // Then
        assertThat(stateHolder.state).isInstanceOf(ProxyChannelState.Closed.class);
        verify(backendHandler).close();
        verify(frontendHandler).closeWithResponse(null);
    }

}