/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;

import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.model.VirtualCluster;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class NewNewProxyFrontendHandlerTest {

    @Mock
    NetFilter netFilter;

    @Mock
    VirtualCluster vc;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    ChannelHandlerContext clientCtx;

    @Mock
    ProxyChannelStateMachine proxyChannelStateMachine;

    AutoCloseable closeable;

    @BeforeEach
    public void openMocks() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    void channelActive() throws Exception {
        // Given
        SaslDecodePredicate dp = new SaslDecodePredicate(false);
        KafkaProxyFrontendHandler handler = new KafkaProxyFrontendHandler(
                netFilter,
                dp,
                vc,
                proxyChannelStateMachine);

        // When
        handler.channelActive(clientCtx);

        // Then
        verify(proxyChannelStateMachine).onClientActive(handler);
        verifyNoInteractions(netFilter);
    }

    @Test
    void channelRead() {
        // Given
        SaslDecodePredicate dp = new SaslDecodePredicate(false);
        HAProxyMessage msg = new HAProxyMessage(
                HAProxyProtocolVersion.V2,
                HAProxyCommand.PROXY,
                HAProxyProxiedProtocol.TCP4,
                "1.1.1.1",
                "2.2.2.2",
                1234,
                4567);
        KafkaProxyFrontendHandler handler = new KafkaProxyFrontendHandler(
                netFilter,
                dp,
                vc,
                proxyChannelStateMachine);

        // When
        handler.channelRead(clientCtx, msg);

        // Then
        verify(proxyChannelStateMachine).onClientRequest(dp, msg);
        verifyNoInteractions(netFilter);
    }

    @Test
    void inSelectingServer() {
        // Given
        SaslDecodePredicate dp = new SaslDecodePredicate(false);
        KafkaProxyFrontendHandler handler = new KafkaProxyFrontendHandler(
                netFilter,
                dp,
                vc,
                proxyChannelStateMachine);

        // When
        handler.inSelectingServer();

        // Then
        verify(netFilter).selectServer(handler);
        verify(proxyChannelStateMachine).assertIsConnecting(anyString());
    }
}
