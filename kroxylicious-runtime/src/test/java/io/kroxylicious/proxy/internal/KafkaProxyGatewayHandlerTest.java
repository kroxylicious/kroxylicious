/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import javax.net.ssl.SSLSession;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class KafkaProxyGatewayHandlerTest {

    @Mock(strictness = Mock.Strictness.LENIENT)
    ProxyChannelStateMachine proxyChannelStateMachine;

    private KafkaProxyGatewayHandler kafkaProxyGatewayHandler;
    private EmbeddedChannel clientChannel;
    private ChannelHandlerContext clientContext;

    @BeforeEach
    void setUp() {
        kafkaProxyGatewayHandler = new KafkaProxyGatewayHandler(proxyChannelStateMachine);
        clientChannel = new EmbeddedChannel();
        clientChannel.pipeline().addFirst(kafkaProxyGatewayHandler);
        clientContext = clientChannel.pipeline().firstContext();
    }

    @Test
    void shouldRejectNullStateMachineInConstructor() {
        assertThatNullPointerException()
                .isThrownBy(() -> new KafkaProxyGatewayHandler(null))
                .withMessage("state machine was null");
    }

    @Test
    void shouldNotifyStateMachineOnChannelActive() throws Exception {
        kafkaProxyGatewayHandler.channelActive(clientContext);

        verify(proxyChannelStateMachine).onClientActive(kafkaProxyGatewayHandler);
    }

    @Test
    void shouldNotifyStateMachineOnChannelInactive() throws Exception {
        kafkaProxyGatewayHandler.channelActive(clientContext);
        kafkaProxyGatewayHandler.channelInactive(clientContext);

        verify(proxyChannelStateMachine).onClientInactive();
    }

    @Test
    void shouldNotifyStateMachineOnSuccessfulSslHandshake() throws Exception {
        SslHandler sslHandler = SslContextBuilder.forClient().build().newHandler(clientChannel.alloc());
        clientChannel.pipeline().addFirst(SslHandler.class.getName(), sslHandler);

        kafkaProxyGatewayHandler.channelActive(clientContext);
        clearInvocations(proxyChannelStateMachine);

        kafkaProxyGatewayHandler.userEventTriggered(clientContext, SslHandshakeCompletionEvent.SUCCESS);

        SSLSession expectedSession = sslHandler.engine().getSession();
        verify(proxyChannelStateMachine).onClientTlsHandshakeSuccess(expectedSession);
    }

    @Test
    void shouldNotNotifyStateMachineOnFailedSslHandshake() throws Exception {
        kafkaProxyGatewayHandler.channelActive(clientContext);
        clearInvocations(proxyChannelStateMachine);

        Exception handshakeFailure = new Exception("TLS handshake failed");
        kafkaProxyGatewayHandler.userEventTriggered(clientContext, new SslHandshakeCompletionEvent(handshakeFailure));

        verifyNoInteractions(proxyChannelStateMachine);
    }

    @Test
    void shouldIgnoreNonSslHandshakeEvents() throws Exception {
        kafkaProxyGatewayHandler.channelActive(clientContext);
        clearInvocations(proxyChannelStateMachine);

        kafkaProxyGatewayHandler.userEventTriggered(clientContext, new Object());

        verifyNoInteractions(proxyChannelStateMachine);
    }

    @Test
    void shouldReturnSslSessionWhenSslHandlerPresent() throws Exception {
        SslHandler sslHandler = SslContextBuilder.forClient().build().newHandler(clientChannel.alloc());
        clientChannel.pipeline().addFirst(SslHandler.class.getName(), sslHandler);

        kafkaProxyGatewayHandler.channelActive(clientContext);

        SSLSession session = kafkaProxyGatewayHandler.sslSession();

        assertThat(session).isNotNull();
        assertThat(session).isSameAs(sslHandler.engine().getSession());
    }

    @Test
    void shouldReturnNullWhenSslHandlerNotPresent() throws Exception {
        kafkaProxyGatewayHandler.channelActive(clientContext);

        SSLSession session = kafkaProxyGatewayHandler.sslSession();

        assertThat(session).isNull();
    }

    @Test
    void shouldThrowWhenContextNotInitialized() {
        KafkaProxyGatewayHandler uninitializedHandler = new KafkaProxyGatewayHandler(proxyChannelStateMachine);

        assertThatNullPointerException()
                .isThrownBy(uninitializedHandler::sslSession);
    }
}
