/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.EmptyByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import io.netty.handler.ssl.SniCompletionEvent;
import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.frame.DecodedFrame;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.OpaqueResponseFrame;
import io.kroxylicious.proxy.internal.KafkaProxyFrontendHandler.State;
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.internal.codec.KafkaRequestDecoder;
import io.kroxylicious.proxy.internal.codec.RequestDecoderTest;
import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.proxy.model.VirtualCluster.DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaProxyFrontendHandlerTest {

    public static final String SNI_HOSTNAME = "external.example.com";
    public static final String CLUSTER_HOST = "internal.example.org";
    public static final int CLUSTER_PORT = 9092;
    public static final String TLS_NEGOTIATION_ERROR = "TLS negotiation error";
    EmbeddedChannel inboundChannel;
    EmbeddedChannel outboundChannel;

    int corrId = 0;
    private final AtomicReference<NetFilter.NetFilterContext> connectContext = new AtomicReference<>();
    private ChannelPromise outboundChannelTcpConnectionFuture;
    private KafkaProxyExceptionMapper exceptionHandler;

    private void writeRequest(short apiVersion, ApiMessage body) {
        var apiKey = ApiKeys.forId(body.apiKey());

        int downstreamCorrelationId = corrId++;

        RequestHeaderData header = new RequestHeaderData()
                                                          .setRequestApiKey(apiKey.id)
                                                          .setRequestApiVersion(apiVersion)
                                                          .setClientId("client-id")
                                                          .setCorrelationId(downstreamCorrelationId);

        inboundChannel.writeInbound(new DecodedRequestFrame<>(apiVersion, corrId, true, header, body));
    }

    @BeforeEach
    public void buildChannel() {
        inboundChannel = new EmbeddedChannel();
        corrId = 0;
        exceptionHandler = new KafkaProxyExceptionMapper();
    }

    @AfterEach
    public void closeChannel() {
        inboundChannel.close();
    }

    public static List<Arguments> provideArgsForExpectedFlow() {
        var result = new ArrayList<Arguments>();
        boolean[] tf = { true, false };
        for (boolean sslConfigured : tf) {
            for (boolean haProxyConfigured : tf) {
                for (boolean saslOffloadConfigured : tf) {
                    for (boolean sendApiVersions : tf) {
                        for (boolean sendSasl : tf) {
                            result.add(Arguments.of(sslConfigured, haProxyConfigured, saslOffloadConfigured, sendApiVersions, sendSasl));
                        }
                    }
                }
            }
        }
        return result;
    }

    /**
     * If the client sends multiple messages immediately when connecting they may all be read
     * from the socket despite the inbound channel auto-read being disabled. We need to tolerate
     * messages being handled while in the CONNECTED/CONNECTING state before the outbound signals
     * it is active.
     */
    @Test
    void testMessageHandledAfterConnectingBeforeConnected() {
        // Given
        KafkaProxyFrontendHandler handler = handler(connectContext::set, new SaslDecodePredicate(false), mock(VirtualCluster.class));
        givenHandlerIsConnecting(handler, "initial");
        writeInboundApiVersionsRequest("post-connecting");

        // When
        whenConnectedAndOutboundBecomesActive(handler);

        // Then
        assertThat(outboundClientSoftwareNames()).containsExactly("initial", "post-connecting");
    }

    @Test
    void testMessageHandledAfterConnectedBeforeOutboundActive() {
        // Given
        KafkaProxyFrontendHandler handler = handler(connectContext::set, new SaslDecodePredicate(false), mock(VirtualCluster.class));
        givenHandlerIsConnected(handler);
        writeInboundApiVersionsRequest("post-connected");

        // When
        outboundChannelBecomesActive(handler);

        // Then
        assertThat(outboundClientSoftwareNames()).containsExactly("initial", "post-connected");
    }

    @Test
    void testUnexpectedMessageReceivedBeforeConnected() {
        // Given
        KafkaProxyFrontendHandler handler = handler(connectContext::set, new SaslDecodePredicate(false), mock(VirtualCluster.class));
        givenHandlerIsConnecting(handler, "initial");

        // When
        Object unexpectedMessage = new Object();
        inboundChannel.writeInbound(unexpectedMessage);

        // Then
        assertEquals(State.FAILED, handler.state());
    }

    @Test
    void testHandleFrameOversizedExceptionDownstreamTlsDisabled() {
        // Given
        VirtualCluster virtualCluster = mock(VirtualCluster.class);
        when(virtualCluster.getDownstreamSslContext()).thenReturn(Optional.empty());
        KafkaProxyFrontendHandler handler = handler(connectContext::set, new SaslDecodePredicate(false), virtualCluster);
        ChannelPipeline pipeline = inboundChannel.pipeline();
        pipeline.addLast(throwOnReadHandler(new DecoderException(new FrameOversizedException(5, 6))));
        pipeline.addLast(handler);

        // when
        inboundChannel.writeInbound(new Object());

        // then
        assertThat(inboundChannel.isOpen()).isFalse();
    }

    @Test
    void testHandleFrameOversizedExceptionDownstreamTlsEnabled() throws SSLException {
        // Given
        VirtualCluster virtualCluster = mock(VirtualCluster.class);
        when(virtualCluster.getDownstreamSslContext()).thenReturn(Optional.of(SslContextBuilder.forClient().build()));
        KafkaProxyFrontendHandler handler = handler(connectContext::set, new SaslDecodePredicate(false), virtualCluster);
        ChannelPipeline pipeline = inboundChannel.pipeline();
        pipeline.addLast(throwOnReadHandler(new DecoderException(new FrameOversizedException(5, 6))));
        pipeline.addLast(handler);

        // when
        inboundChannel.writeInbound(new Object());

        // then
        assertThat(inboundChannel.isOpen()).isFalse();
    }

    @Test
    void testUnexpectedMessageReceivedBeforeOutboundActive() {
        // Given
        KafkaProxyFrontendHandler handler = handler(connectContext::set, new SaslDecodePredicate(false), mock(VirtualCluster.class));
        givenHandlerIsConnected(handler);

        // When
        Object unexpectedMessage = new Object();
        inboundChannel.writeInbound(unexpectedMessage);

        // Then
        assertEquals(State.FAILED, handler.state());
    }

    private void writeInboundApiVersionsRequest(String clientSoftwareName) {
        writeRequest(
                ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION,
                new ApiVersionsRequestData()
                                            .setClientSoftwareName(clientSoftwareName)
                                            .setClientSoftwareVersion("1.0.0")
        );
    }

    KafkaProxyFrontendHandler handler(NetFilter filter, SaslDecodePredicate dp, VirtualCluster virtualCluster) {
        return new KafkaProxyFrontendHandler(filter, dp, virtualCluster, exceptionHandler) {
            @Override
            ChannelFuture initConnection(String remoteHost, int remotePort, Bootstrap b) {
                // This is ugly... basically the EmbeddedChannel doesn't seem to handle the case
                // of a handler creating an outgoing connection and ends up
                // trying to re-register the outbound channel => IllegalStateException
                // So we override this method to short-circuit that
                outboundChannel = new EmbeddedChannel();
                outboundChannelTcpConnectionFuture = outboundChannel.newPromise();
                return outboundChannelTcpConnectionFuture.addListener(
                        future -> this.onUpstreamChannelActive(outboundChannel.pipeline().firstContext().fireChannelActive())
                );
            }
        };
    }

    /**
     * Test the normal flow, in a number of configurations.
     *
     * @param sslConfigured         Whether SSL is configured
     * @param haProxyConfigured
     * @param saslOffloadConfigured
     * @param sendApiVersions
     * @param sendSasl
     */
    @ParameterizedTest
    @MethodSource("provideArgsForExpectedFlow")
    void expectedFlow(
            boolean sslConfigured,
            boolean haProxyConfigured,
            boolean saslOffloadConfigured,
            boolean sendApiVersions,
            boolean sendSasl
    ) {

        var dp = new SaslDecodePredicate(saslOffloadConfigured);
        ArgumentCaptor<NetFilter.NetFilterContext> valueCapture = ArgumentCaptor.forClass(NetFilter.NetFilterContext.class);
        var filter = mock(NetFilter.class);
        var virtualCluster = mock(VirtualCluster.class);
        when(virtualCluster.getUpstreamSslContext()).thenReturn(Optional.empty());

        doAnswer(i -> {
            NetFilter.NetFilterContext ctx = i.getArgument(0);
            if (sslConfigured) {
                assertEquals(SNI_HOSTNAME, ctx.sniHostname());
            } else {
                assertNull(ctx.sniHostname());
            }
            if (haProxyConfigured) {
                assertEquals("embedded", String.valueOf(ctx.srcAddress()));
                assertEquals("1.2.3.4", ctx.clientHost());
            } else {
                assertEquals("embedded", String.valueOf(ctx.srcAddress()));
                assertEquals("embedded", ctx.clientHost());
            }
            if (sendApiVersions) {
                assertEquals("foo", ctx.clientSoftwareName());
                assertEquals("1.0.0", ctx.clientSoftwareVersion());
            } else {
                assertNull(ctx.clientSoftwareName());
                assertNull(ctx.clientSoftwareVersion());
            }
            if (saslOffloadConfigured && sendSasl) {
                assertEquals("alice", ctx.authorizedId());
            } else {
                assertNull(ctx.authorizedId());
            }

            connectionInitiated(ctx);
            return null;
        }).when(filter).selectServer(valueCapture.capture());

        var handler = handler(filter, dp, virtualCluster);
        initialiseInboundChannel(handler);

        if (sslConfigured) {
            // Simulate the SSL handler
            inboundChannel.pipeline().fireUserEventTriggered(new SniCompletionEvent(SNI_HOSTNAME));
        }

        if (haProxyConfigured) {
            // Simulate the HA proxy handler
            inboundChannel.writeInbound(
                    new HAProxyMessage(
                            HAProxyProtocolVersion.V1,
                            HAProxyCommand.PROXY,
                            HAProxyProxiedProtocol.TCP4,
                            "1.2.3.4",
                            "5.6.7.8",
                            65535,
                            CLUSTER_PORT
                    )
            );
        }

        if (sendApiVersions) {
            // Simulate the client doing ApiVersions
            writeInboundApiVersionsRequest("foo");
            if (saslOffloadConfigured) {
                assertEquals(State.API_VERSIONS, handler.state());
                // when offloading SASL, we do not connect to a backend server until after SASL auth. The ApiVersions response is generated in the proxy.
                verify(filter, never()).selectServer(handler);
            } else {
                // should cause connection to the backend cluster when not offloading SASL
                handleConnect(filter, handler);
            }
        }

        if (sendSasl) {
            if (saslOffloadConfigured) {
                // Simulate the KafkaAuthnHandler having done SASL offload
                inboundChannel.pipeline().fireUserEventTriggered(new AuthenticationEvent("alice", Map.of()));

                // If the pipeline is configured for SASL offload (KafkaAuthnHandler)
                // then we assume it DOESN'T propagate the SASL frames down the pipeline
                // and therefore no backend connection happens
                verify(filter, never()).selectServer(handler);
            } else {
                // Simulate the client doing SaslHandshake and SaslAuthentication,
                writeRequest(SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION, new SaslHandshakeRequestData());
                if (!sendApiVersions) {
                    // client doesn't send api versions, so the next frame drives selectServer
                    handleConnect(filter, handler);
                }
                writeRequest(SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION, new SaslAuthenticateRequestData());
            }
        }

        // Simulate a Metadata request
        writeRequest(MetadataRequestData.HIGHEST_SUPPORTED_VERSION, new MetadataRequestData());
        if (sendSasl && saslOffloadConfigured) {
            handleConnect(filter, handler);
        }

    }

    @Test
    void shouldCloseWithMappedResponseChannelOnFailedConnection() {
        // Given
        var resp = new OpaqueResponseFrame(Unpooled.EMPTY_BUFFER, 42, 0);
        KafkaProxyFrontendHandler handler = handler(connectContext::set, new SaslDecodePredicate(false), mock(VirtualCluster.class));
        exceptionHandler.registerExceptionResponse(SSLHandshakeException.class, throwable -> Optional.of(resp));

        // When
        handler.onUpstreamChannelFailed(new HostPort("wibble", 9092), inboundChannel.newFailedFuture(new SSLHandshakeException(TLS_NEGOTIATION_ERROR)), inboundChannel);

        // Then
        assertThat(inboundChannel.<OpaqueResponseFrame> readOutbound()).isEqualTo(resp);
        assertThat(inboundChannel.isOpen()).isFalse();
    }

    @Test
    void shouldCloseResponseChannelOnFailedConnection() {
        // Given
        var resp = new OpaqueResponseFrame(Unpooled.EMPTY_BUFFER, 42, 0);
        KafkaProxyFrontendHandler handler = handler(connectContext::set, new SaslDecodePredicate(false), mock(VirtualCluster.class));
        exceptionHandler.registerExceptionResponse(SSLHandshakeException.class, throwable -> Optional.of(resp));

        // When
        handler.onUpstreamChannelFailed(new HostPort("wibble", 9092), inboundChannel.newFailedFuture(new RuntimeException(TLS_NEGOTIATION_ERROR)), inboundChannel);

        // Then
        assertThat(inboundChannel.<ByteBuf> readOutbound()).isNotNull().isExactlyInstanceOf(EmptyByteBuf.class);
        assertThat(inboundChannel.isOpen()).isFalse();
    }

    private void initialiseInboundChannel(KafkaProxyFrontendHandler handler) {
        final ChannelPipeline pipeline = inboundChannel.pipeline();
        if (pipeline.get(KafkaProxyFrontendHandler.class) == null) {
            pipeline.addLast(handler);
        }
        assertEquals(State.START, handler.state());

        pipeline.fireChannelActive();
    }

    private void handleConnect(NetFilter filter, KafkaProxyFrontendHandler handler) {
        verify(filter).selectServer(handler);
        assertEquals(State.CONNECTING, handler.state());
        assertFalse(
                inboundChannel.config().isAutoRead(),
                "Expect inbound autoRead=true, since outbound not yet active"
        );

        // Simulate the backend handler receiving channel active and telling the frontend handler
        outboundChannelBecomesActive(handler);
    }

    private void outboundChannelBecomesActive(KafkaProxyFrontendHandler handler) {
        outboundChannelTcpConnectionFuture.setSuccess();
        assertTrue(
                inboundChannel.config().isAutoRead(),
                "Expect inbound autoRead=true, since outbound now active"
        );
        assertEquals(State.OUTBOUND_ACTIVE, handler.state());
    }

    private List<String> outboundClientSoftwareNames() {
        return outboundFrames(ApiVersionsRequestData.class).stream().map(DecodedFrame::body).map(ApiVersionsRequestData::clientSoftwareName).toList();
    }

    private <T extends ApiMessage> List<DecodedRequestFrame<T>> outboundFrames(Class<T> ignored) {
        ByteBuf outboundMessage;
        List<DecodedRequestFrame<T>> result = new ArrayList<>();
        while ((outboundMessage = outboundChannel.readOutbound()) != null) {
            assertThat(outboundMessage).isNotNull();
            ArrayList<Object> objects = new ArrayList<>();
            new KafkaRequestDecoder(RequestDecoderTest.DECODE_EVERYTHING, DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES).decode(
                    outboundChannel.pipeline().firstContext(),
                    outboundMessage,
                    objects
            );
            assertThat(objects).hasSize(1);
            if (objects.get(0) instanceof DecodedRequestFrame<?> f) {
                // noinspection unchecked
                result.add((DecodedRequestFrame<T>) f);
            } else {
                throw new IllegalStateException("message was not a DecodedRequestFrame");
            }
        }
        return result;
    }

    private void whenConnectedAndOutboundBecomesActive(KafkaProxyFrontendHandler handler) {
        connectionInitiated(connectContext.get());
        assertEquals(State.CONNECTING, handler.state());
        outboundChannelBecomesActive(handler);
        assertEquals(State.OUTBOUND_ACTIVE, handler.state());
    }

    private void givenHandlerIsConnected(KafkaProxyFrontendHandler handler) {
        givenHandlerIsConnecting(handler, "initial");
        connectionInitiated(connectContext.get());
        assertEquals(State.CONNECTING, handler.state());
    }

    private void connectionInitiated(NetFilter.NetFilterContext connectContext) {
        connectContext.initiateConnect(new HostPort(CLUSTER_HOST, CLUSTER_PORT), List.of());
    }

    private void givenHandlerIsConnecting(KafkaProxyFrontendHandler handler, String initialClientSoftwareName) {
        initialiseInboundChannel(handler);
        writeInboundApiVersionsRequest(initialClientSoftwareName);
        assertEquals(State.CONNECTING, handler.state());
    }

    @NonNull
    private static ChannelInboundHandlerAdapter throwOnReadHandler(Exception cause) {
        return new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                ctx.fireExceptionCaught(cause);
            }
        };
    }

}
