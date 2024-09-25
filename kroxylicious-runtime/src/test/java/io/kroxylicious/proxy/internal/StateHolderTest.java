/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.List;
import java.util.Optional;

import javax.net.ssl.SSLException;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.internal.ProxyChannelState.ApiVersions;
import io.kroxylicious.proxy.internal.ProxyChannelState.SelectingServer;
import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class StateHolderTest {

    private StateHolder stateHolder;
    private KafkaProxyBackendHandler backendHandler;
    private KafkaProxyFrontendHandler frontendHandler;
    private ApiVersions emptyApiVersionsState;

    @BeforeEach
    void setUp() {
        stateHolder = new StateHolder();
        backendHandler = mock(KafkaProxyBackendHandler.class);
        frontendHandler = mock( KafkaProxyFrontendHandler.class);
        emptyApiVersionsState = new ApiVersions(null, null, null);
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
    void shouldCallInClientActive() {
        // Given

        // When
        stateHolder.onClientActive(frontendHandler);

        // Then
        assertThat(stateHolder.state).isInstanceOf(ProxyChannelState.ClientActive.class);
        verify(frontendHandler, times(1)).inClientActive();
    }

    @Test
    void shouldCloseOnClientActiveInInvalidState() {
        // Given
        stateHolder.state = emptyApiVersionsState;
        stateHolder.backendHandler = null;
        stateHolder.frontendHandler = null;

        // When
        stateHolder.onClientActive(frontendHandler);

        // Then
        assertThat(stateHolder.state).isInstanceOf(ProxyChannelState.Closed.class);
        verifyNoInteractions(frontendHandler);
        verifyNoInteractions(backendHandler);
    }

    @Test
    void shouldCaptureHaProxyState() {
        // Given
        HAProxyMessage haProxyMessage = new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
                "1.1.1.1", "2.2.2.2", 46421, 9092);
        stateHolder.state = new ProxyChannelState.ClientActive();
        stateHolder.backendHandler = null;
        stateHolder.frontendHandler = frontendHandler;

        // When
        stateHolder.onClientRequest(null, haProxyMessage);

        // Then
        assertThat(stateHolder.state)
                .asInstanceOf(InstanceOfAssertFactories.type(ProxyChannelState.HaProxy.class))
                .extracting(ProxyChannelState.HaProxy::haProxyMessage)
                .isSameAs(haProxyMessage);
    }

    @Test
    void shouldCloseOnHaProxyMessageInInvalidState() {
        // Given
        HAProxyMessage haProxyMessage = new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
                "1.1.1.1", "2.2.2.2", 46421, 9092);
        stateHolder.state = emptyApiVersionsState;
        stateHolder.backendHandler = null;
        stateHolder.frontendHandler = frontendHandler;

        // When
        stateHolder.onClientRequest(null, haProxyMessage);

        // Then
        assertThat(stateHolder.state).isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).closeWithResponse(null);
        verifyNoInteractions(backendHandler);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldTransitionToApiVersionsOrSelectingServerFromClientActive(boolean handlingSasl) {
        // Given
        stateHolder.state = new ProxyChannelState.ClientActive();
        stateHolder.backendHandler = null;
        stateHolder.frontendHandler = frontendHandler;

        // When
        stateHolder.onClientRequest(
                new SaslDecodePredicate(handlingSasl),
                new DecodedRequestFrame<>(
                        ApiVersionsResponseData.ApiVersion.HIGHEST_SUPPORTED_VERSION,
                        1,
                        false,
                        new RequestHeaderData(),
                        new ApiVersionsRequestData()
                                .setClientSoftwareName("mykafkalib")
                                .setClientSoftwareVersion("1.0.0")));

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
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldTransitionToConnectingWhenOnServerSelectedCalled(boolean configureSsl) throws SSLException {
        // Given
        HostPort brokerAddress = new HostPort("localhost", 9092);
        stateHolder.state = new ProxyChannelState.SelectingServer(null, null, null);
        stateHolder.backendHandler = null;
        stateHolder.frontendHandler = frontendHandler;
        var filters = List.<FilterAndInvoker>of();
        var vc = mock(VirtualCluster.class);
        doReturn(configureSsl ? Optional.of(SslContextBuilder.forClient().build()) : Optional.empty()).when(vc).getUpstreamSslContext();
        var nf = mock(NetFilter.class);

        // When
        stateHolder.onServerSelected(brokerAddress, filters, vc, nf);

        // Then
        assertThat(stateHolder.state)
                .asInstanceOf(InstanceOfAssertFactories.type(ProxyChannelState.Connecting.class))
                .extracting(ProxyChannelState.Connecting::remote).isEqualTo(brokerAddress);
        verify(frontendHandler).inConnecting(eq(brokerAddress), eq(filters), notNull(KafkaProxyBackendHandler.class));
        assertThat(stateHolder.backendHandler).isNotNull();
    }

    @Test
    void shouldCloseWhenOnServerSelectedCalledInInvalidState() {
        // Given
        HostPort brokerAddress = new HostPort("localhost", 9092);
        stateHolder.state = new ProxyChannelState.ClientActive();
        stateHolder.backendHandler = null;
        stateHolder.frontendHandler = frontendHandler;
        var filters = List.<FilterAndInvoker>of();
        var vc = mock(VirtualCluster.class);
        var nf = mock(NetFilter.class);

        // When
        stateHolder.onServerSelected(brokerAddress, filters, vc, nf);

        // Then
        assertThat(stateHolder.state)
                .isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).closeWithResponse(null);
        assertThat(stateHolder.backendHandler).isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldTransitionWhenOnServerActiveCalledInConnectingState(boolean configureTls) throws SSLException {
        // Given
        HostPort brokerAddress = new HostPort("localhost", 9092);
        stateHolder.state = new ProxyChannelState.Connecting(null, null, null, brokerAddress);
        stateHolder.backendHandler = backendHandler;
        stateHolder.frontendHandler = frontendHandler;
        var filters = List.<FilterAndInvoker>of();
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
                    .extracting(ProxyChannelState.NegotiatingTls::remote).isEqualTo(brokerAddress);
            verifyNoInteractions(frontendHandler);
            verifyNoInteractions(backendHandler);
        }
        else {
            var stateAssert = assertThat(stateHolder.state)
                    .asInstanceOf(InstanceOfAssertFactories.type(ProxyChannelState.Forwarding.class));
            stateAssert
                    .extracting(ProxyChannelState.Forwarding::remote).isEqualTo(brokerAddress);
            verify(frontendHandler).inForwarding();
            verifyNoInteractions(backendHandler);
        }
    }

    @Test
    void shouldCloseWhenOnServerActiveCalledInIllegalState() throws SSLException {
        // Given
        HostPort brokerAddress = new HostPort("localhost", 9092);
        stateHolder.state = new ProxyChannelState.ClientActive();
        stateHolder.backendHandler = backendHandler;
        stateHolder.frontendHandler = frontendHandler;
        var filters = List.<FilterAndInvoker>of();
        var serverCtx = mock(ChannelHandlerContext.class);
        SslContext sslContext = null;

        // When
        stateHolder.onServerActive(serverCtx, sslContext);

        // Then
        assertThat(stateHolder.state)
                .isInstanceOf(ProxyChannelState.Closed.class);
        verify(frontendHandler).closeWithResponse(null);
        verify(backendHandler).close();
    }
}