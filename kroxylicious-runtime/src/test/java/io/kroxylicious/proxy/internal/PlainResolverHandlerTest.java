/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;

import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointBindingResolver;
import io.kroxylicious.proxy.internal.net.EndpointResolutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PlainResolverHandlerTest {

    private static final String CHANNEL_ACTIVE = "channelActive";
    private static final String CHANNEL_READ_COMPLETE = "channelReadComplete";
    private static final String CONNECTION_INITIALISED = "connectionInitialised";

    @Mock
    private EndpointBindingResolver bindingResolver;

    @Mock
    private EndpointBinding binding;

    @Mock
    private BiConsumer<Channel, EndpointBinding> connectionInitializer;

    private CompletableFuture<EndpointBinding> bindingFuture;
    private Counter clientToProxyErrorCounter;
    private PlainResolverHandler handler;
    private RecordingHandler downstream;
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        bindingFuture = new CompletableFuture<>();
        when(bindingResolver.resolve(any(Channel.class), isNull())).thenReturn(bindingFuture);
        clientToProxyErrorCounter = new SimpleMeterRegistry().counter("client_to_proxy_errors");
        handler = new PlainResolverHandler(bindingResolver, connectionInitializer, clientToProxyErrorCounter);
        downstream = new RecordingHandler();
        // The channel is created without handlers so that its initial channelActive event is not seen by
        // the handler under test; tests fire that event explicitly using activateChannel().
        channel = new EmbeddedChannel();
        channel.pipeline().addLast(handler, downstream);
    }

    @AfterEach
    void tearDown() {
        // messages replayed to the recording handler are owned by the test
        downstream.events.forEach(ReferenceCountUtil::release);
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldResolveBindingWhenChannelBecomesActive() {
        // When
        activateChannel();

        // Then
        verify(bindingResolver).resolve(channel, null);
    }

    @Test
    void shouldWithholdChannelActiveWhileBindingPending() {
        // When
        activateChannel();

        // Then
        assertThat(downstream.events).isEmpty();
    }

    @Test
    void shouldBufferReadsWhileBindingPending() {
        // Given
        activateChannel();

        // When
        channel.pipeline().fireChannelRead("first");

        // Then
        assertThat(downstream.events).isEmpty();
    }

    @Test
    void shouldInitialiseConnectionWhenBindingResolves() {
        // Given
        activateChannel();

        // When
        resolveBinding();

        // Then
        verify(connectionInitializer).accept(channel, binding);
    }

    @Test
    void shouldInitialiseConnectionWhenBindingAlreadyResolvedOnActivation() {
        // Given
        bindingFuture.complete(binding);

        // When
        activateChannel();

        // Then
        verify(connectionInitializer).accept(channel, binding);
    }

    @Test
    void shouldDeferConnectionInitialisationToEventLoop() {
        // Given
        activateChannel();

        // When
        bindingFuture.complete(binding);

        // Then
        verifyNoInteractions(connectionInitializer);
        assertThat(channel.hasPendingTasks()).isTrue();
    }

    @Test
    void shouldInitialiseConnectionBeforeFiringChannelActive() {
        // Given
        doAnswer(invocation -> {
            downstream.events.add(CONNECTION_INITIALISED);
            return null;
        }).when(connectionInitializer).accept(channel, binding);
        activateChannel();

        // When
        resolveBinding();

        // Then
        assertThat(downstream.events).containsExactly(CONNECTION_INITIALISED, CHANNEL_ACTIVE);
    }

    @Test
    void shouldFireChannelActiveThenReplayBufferedReadsInOrderWhenBindingResolves() {
        // Given
        activateChannel();
        channel.pipeline().fireChannelRead("first");
        channel.pipeline().fireChannelRead("second");

        // When
        resolveBinding();

        // Then
        assertThat(downstream.events).containsExactly(CHANNEL_ACTIVE, "first", "second", CHANNEL_READ_COMPLETE);
    }

    @Test
    void shouldNotFireChannelReadCompleteWhenNothingBuffered() {
        // Given
        activateChannel();

        // When
        resolveBinding();

        // Then
        assertThat(downstream.events).containsExactly(CHANNEL_ACTIVE);
    }

    @Test
    void shouldNotReleaseReplayedMessages() {
        // Given
        ByteBuf buf = Unpooled.buffer(4).writeInt(42);
        activateChannel();
        channel.pipeline().fireChannelRead(buf);

        // When
        resolveBinding();

        // Then
        assertThat(buf.refCnt()).isOne();
    }

    @Test
    void shouldRemoveItselfFromPipelineWhenBindingResolves() {
        // Given
        activateChannel();

        // When
        resolveBinding();

        // Then
        assertThat(channel.pipeline().get(PlainResolverHandler.class)).isNull();
    }

    @Test
    void shouldForwardReadsDirectlyOnceBindingResolved() {
        // Given
        activateChannel();
        resolveBinding();
        downstream.events.clear();

        // When
        channel.pipeline().fireChannelRead("later");

        // Then
        assertThat(downstream.events).containsExactly("later");
    }

    @Test
    void shouldCloseChannelWhenBindingResolutionFails() {
        // Given
        activateChannel();

        // When
        failBindingResolution();

        // Then
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void shouldCountErrorWhenBindingResolutionFails() {
        // Given
        activateChannel();

        // When
        failBindingResolution();

        // Then
        assertThat(clientToProxyErrorCounter.count()).isEqualTo(1.0);
    }

    @Test
    void shouldNotInitialiseConnectionWhenBindingResolutionFails() {
        // Given
        activateChannel();
        channel.pipeline().fireChannelRead("first");

        // When
        failBindingResolution();

        // Then
        verifyNoInteractions(connectionInitializer);
        assertThat(downstream.events).isEmpty();
    }

    @Test
    void shouldReleaseBufferedMessagesWhenBindingResolutionFails() {
        // Given
        ByteBuf buf = Unpooled.buffer(4).writeInt(42);
        activateChannel();
        channel.pipeline().fireChannelRead(buf);

        // When
        failBindingResolution();

        // Then
        assertThat(buf.refCnt()).isZero();
    }

    @Test
    void shouldReleaseBufferedMessagesWhenHandlerRemoved() {
        // Given
        ByteBuf buf = Unpooled.buffer(4).writeInt(42);
        activateChannel();
        channel.pipeline().fireChannelRead(buf);

        // When
        channel.pipeline().remove(handler);

        // Then
        assertThat(buf.refCnt()).isZero();
    }

    @Test
    void shouldFireExceptionCaughtWhenConnectionInitialisationFails() {
        // Given
        var failure = new IllegalStateException("initialisation failed");
        doThrow(failure).when(connectionInitializer).accept(channel, binding);
        activateChannel();

        // When
        resolveBinding();

        // Then
        assertThat(downstream.events).containsExactly(failure);
    }

    @Test
    void shouldRemoveItselfFromPipelineWhenConnectionInitialisationFails() {
        // Given
        doThrow(new IllegalStateException("initialisation failed")).when(connectionInitializer).accept(channel, binding);
        activateChannel();

        // When
        resolveBinding();

        // Then
        assertThat(channel.pipeline().get(PlainResolverHandler.class)).isNull();
    }

    @Test
    void shouldReleaseBufferedMessagesWhenConnectionInitialisationFails() {
        // Given
        ByteBuf buf = Unpooled.buffer(4).writeInt(42);
        doThrow(new IllegalStateException("initialisation failed")).when(connectionInitializer).accept(channel, binding);
        activateChannel();
        channel.pipeline().fireChannelRead(buf);

        // When
        resolveBinding();

        // Then
        assertThat(buf.refCnt()).isZero();
    }

    private void activateChannel() {
        channel.pipeline().fireChannelActive();
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
     * Stands in for the handlers that follow the resolver in the pipeline, recording the inbound
     * events that the resolver lets through, in the order they arrive.
     */
    private static class RecordingHandler extends ChannelInboundHandlerAdapter {
        private final List<Object> events = new ArrayList<>();

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            events.add(CHANNEL_ACTIVE);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            events.add(msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            events.add(CHANNEL_READ_COMPLETE);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            events.add(cause);
        }
    }
}
