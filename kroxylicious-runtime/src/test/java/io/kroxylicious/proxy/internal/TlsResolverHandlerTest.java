/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;

import io.github.sambarker.logsquelcher.CapturedLogs;
import io.github.sambarker.logsquelcher.LogSquelcherExtension;
import io.github.sambarker.logsquelcher.LoggingEventAssert;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.internal.StringUtil;

import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointBindingResolver;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.net.EndpointResolutionException;
import io.kroxylicious.proxy.internal.util.TlsTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@ExtendWith(LogSquelcherExtension.class)
class TlsResolverHandlerTest {

    private static final String CHANNEL_ACTIVE = "channelActive";
    private static final String CONNECTION_INITIALISED = "connectionInitialised";

    // Using SNI test data from Netty
    // https://github.com/netty/netty/blob/57bea3bea22717639ba200432c81b23154e4bbd9/handler/src/test/java/io/netty/handler/ssl/SniHandlerTest.java#L222
    // hex dump of a client hello packet, which contains hostname "CHAT4.LEANCLOUD.CN"
    private static final String SNI_HOSTNAME = "chat4.leancloud.cn";
    private static final String TLS_HANDSHAKE_MESSAGE_HEX_1 = "16030100";
    // part 2
    private static final String TLS_HANDSHAKE_MESSAGE_HEX_2 = "c6010000c20303bb0855d66532c05a0ef784f7c384feeafa68b3" +
            "b655ac7288650d5eed4aa3fb52000038c02cc030009fcca9cca8ccaac02b" +
            "c02f009ec024c028006bc023c0270067c00ac0140039c009c0130033009d" +
            "009c003d003c0035002f00ff010000610000001700150000124348415434" +
            "2e4c45414e434c4f55442e434e000b000403000102000a000a0008001d00" +
            "170019001800230000000d0020001e060106020603050105020503040104" +
            "0204030301030203030201020202030016000000170000";

    private static SslContext serverSslContext;

    @Mock
    private EndpointBindingResolver bindingResolver;

    @Mock
    private EndpointBinding binding;

    @Mock
    private EndpointGateway gateway;

    @Mock
    private BiConsumer<Channel, EndpointBinding> connectionInitializer;

    private CompletableFuture<EndpointBinding> bindingFuture;
    private Counter clientToProxyErrorCounter;
    private RecordingHandler downstream;
    private EmbeddedChannel channel;

    @BeforeAll
    static void buildServerSslContext() throws Exception {
        var keyAndCert = TlsTestUtils.generateKeyAndCert(TlsTestUtils.Algorithm.RSA);
        serverSslContext = SslContextBuilder.forServer(
                new ByteArrayInputStream(keyAndCert.certificatePem().getBytes(StandardCharsets.US_ASCII)),
                new ByteArrayInputStream(keyAndCert.privateKeyPem().getBytes(StandardCharsets.US_ASCII)))
                .build();
    }

    @BeforeEach
    void setUp() {
        bindingFuture = new CompletableFuture<>();
        clientToProxyErrorCounter = new SimpleMeterRegistry().counter("client_to_proxy_errors");
        downstream = new RecordingHandler();
        channel = new EmbeddedChannel();
        channel.pipeline().addLast(new TlsResolverHandler(channel, bindingResolver, connectionInitializer, clientToProxyErrorCounter), downstream);
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldResolveBindingForSniHostname() {
        // Given
        givenBindingResolutionPending();

        // When
        sendClientHello();

        // Then
        verify(bindingResolver).resolve(channel, SNI_HOSTNAME);
    }

    @Test
    void shouldWithholdChannelActiveWhileBindingPending() {
        // Given
        givenBindingResolutionPending();

        // When
        sendClientHello();

        // Then
        assertThat(downstream.events).isEmpty();
        verifyNoInteractions(connectionInitializer);
    }

    @Test
    void shouldInitialiseConnectionWhenBindingResolves() {
        // Given
        givenGatewayProvidesSslContext();
        givenBindingResolutionPending();
        sendClientHello();

        // When
        resolveBinding();

        // Then
        verify(connectionInitializer).accept(channel, binding);
    }

    @Test
    void shouldInitialiseConnectionBeforeFiringChannelActive() {
        // Given
        doAnswer(invocation -> {
            downstream.events.add(CONNECTION_INITIALISED);
            return null;
        }).when(connectionInitializer).accept(channel, binding);
        givenGatewayProvidesSslContext();
        givenBindingResolutionPending();
        sendClientHello();

        // When
        resolveBinding();

        // Then
        assertThat(downstream.events).startsWith(CONNECTION_INITIALISED, CHANNEL_ACTIVE);
    }

    @Test
    void shouldReplaceItselfWithSslHandlerWhenBindingResolves() {
        // Given
        givenGatewayProvidesSslContext();
        givenBindingResolutionPending();
        sendClientHello();

        // When
        resolveBinding();

        // Then
        assertThat(channel.pipeline().get(TlsResolverHandler.class)).isNull();
        assertThat(channel.pipeline().get(SslHandler.class)).isNotNull();
    }

    @Test
    void shouldKeepChannelOpenWhenBindingResolves() {
        // Given
        givenGatewayProvidesSslContext();
        givenBindingResolutionPending();
        sendClientHello();

        // When
        resolveBinding();

        // Then
        assertThat(channel.isOpen()).isTrue();
        assertThat(clientToProxyErrorCounter.count()).isZero();
    }

    @Test
    void shouldCloseChannelWhenBindingResolutionFails() {
        // Given
        givenBindingResolutionPending();
        sendClientHello();

        // When
        failBindingResolution();

        // Then
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void shouldCountErrorWhenBindingResolutionFails() {
        // Given
        givenBindingResolutionPending();
        sendClientHello();

        // When
        failBindingResolution();

        // Then
        assertThat(clientToProxyErrorCounter.count()).isPositive();
    }

    @Test
    void shouldNotInitialiseConnectionWhenBindingResolutionFails() {
        // Given
        givenBindingResolutionPending();
        sendClientHello();

        // When
        failBindingResolution();

        // Then
        verifyNoInteractions(connectionInitializer);
        assertThat(downstream.events).isEmpty();
    }

    @Test
    void shouldLogWarningWhenBindingResolutionFails(CapturedLogs logs) {
        // Given
        givenBindingResolutionPending();
        sendClientHello();

        // When
        failBindingResolution();

        // Then
        LoggingEventAssert.assertThat(logs.logged(TlsResolverHandler.class))
                .anyMatch(event -> event.getLevel().equals(Level.WARN) && event.getMessage().contains("Exception resolving Virtual Cluster Binding"));
    }

    @Test
    void shouldCloseChannelWhenBindingResolverThrows() {
        // Given
        when(bindingResolver.resolve(any(Channel.class), any(String.class))).thenThrow(new IllegalStateException("resolver failed"));

        // When
        sendClientHello();

        // Then
        assertThat(channel.isOpen()).isFalse();
        assertThat(clientToProxyErrorCounter.count()).isPositive();
    }

    @Test
    void shouldCloseChannelWhenGatewayProvidesNoSslContext() {
        // Given
        when(binding.endpointGateway()).thenReturn(gateway);
        when(gateway.getDownstreamSslContext()).thenReturn(Optional.empty());
        givenBindingResolutionPending();
        sendClientHello();

        // When
        resolveBinding();

        // Then
        assertThat(channel.isOpen()).isFalse();
        verifyNoInteractions(connectionInitializer);
    }

    @Test
    void shouldCloseChannelWhenConnectionInitialisationFails() {
        // Given
        doThrow(new IllegalStateException("initialisation failed")).when(connectionInitializer).accept(channel, binding);
        givenGatewayProvidesSslContext();
        givenBindingResolutionPending();
        sendClientHello();

        // When
        resolveBinding();

        // Then
        assertThat(channel.isOpen()).isFalse();
        assertThat(downstream.events).isEmpty();
    }

    private void givenBindingResolutionPending() {
        when(bindingResolver.resolve(any(Channel.class), any(String.class))).thenReturn(bindingFuture);
    }

    private void givenGatewayProvidesSslContext() {
        when(binding.endpointGateway()).thenReturn(gateway);
        when(gateway.getDownstreamSslContext()).thenReturn(Optional.of(serverSslContext));
    }

    private void sendClientHello() {
        channel.writeInbound(Unpooled.wrappedBuffer(StringUtil.decodeHexDump(TLS_HANDSHAKE_MESSAGE_HEX_1)),
                Unpooled.wrappedBuffer(StringUtil.decodeHexDump(TLS_HANDSHAKE_MESSAGE_HEX_2)));
        channel.runPendingTasks();
    }

    private void resolveBinding() {
        bindingFuture.complete(binding);
        channel.runPendingTasks();
    }

    private void failBindingResolution() {
        bindingFuture.completeExceptionally(new EndpointResolutionException("not resolved"));
        channel.runPendingTasks();
    }

    /**
     * Stands in for the handlers that follow the resolver in the pipeline, recording the
     * channelActive events and exceptions that reach them, in the order they arrive.
     */
    private static class RecordingHandler extends ChannelInboundHandlerAdapter {
        private final List<Object> events = new ArrayList<>();

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            events.add(CHANNEL_ACTIVE);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            events.add(cause);
        }
    }
}
