/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

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

import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.internal.ProxyChannelState.ApiVersions;
import io.kroxylicious.proxy.internal.ProxyChannelState.SelectingServer;

import static org.assertj.core.api.Assertions.assertThat;
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
}