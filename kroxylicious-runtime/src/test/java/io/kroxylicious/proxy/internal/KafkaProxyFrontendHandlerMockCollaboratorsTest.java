/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import io.netty.handler.timeout.IdleStateHandler;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.config.CacheConfiguration;
import io.kroxylicious.proxy.config.NettySettings;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.internal.filter.TopicNameCacheFilter;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.internal.subject.DefaultSubjectBuilder;
import io.kroxylicious.proxy.model.VirtualClusterModel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaProxyFrontendHandlerMockCollaboratorsTest {

    public static final DelegatingDecodePredicate DELEGATING_PREDICATE = new DelegatingDecodePredicate();
    public static final NettySettings NETTY_SETTINGS = new NettySettings(Optional.empty(), Optional.empty(), Optional.of(Duration.ofSeconds(33)), Optional.empty());
    private static final String CLUSTER_NAME = "TestCluster";

    @Mock(strictness = Mock.Strictness.LENIENT)
    private ChannelPipeline channelPipeline;

    @Mock
    PluginFactoryRegistry pfr;

    @Mock
    FilterChainFactory filterChainFactory;

    @Mock
    EndpointReconciler endpointReconciler;

    @Mock(strictness = Mock.Strictness.LENIENT)
    VirtualClusterModel virtualCluster;

    @Mock(strictness = Mock.Strictness.LENIENT)
    EndpointBinding endpointBinding;

    @Mock(strictness = Mock.Strictness.LENIENT)
    EndpointGateway endpointGateway;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    ChannelHandlerContext clientCtx;

    @Mock(strictness = Mock.Strictness.LENIENT)
    ProxyChannelStateMachine proxyChannelStateMachine;
    private KafkaProxyFrontendHandler handler;

    @BeforeEach
    void setUp() {
        when(endpointGateway.virtualCluster()).thenReturn(virtualCluster);
        when(endpointBinding.endpointGateway()).thenReturn(endpointGateway);
        when(proxyChannelStateMachine.endpointBinding()).thenReturn(endpointBinding);
        when(proxyChannelStateMachine.virtualCluster()).thenReturn(virtualCluster);
        handler = new KafkaProxyFrontendHandler(
                pfr,
                filterChainFactory,
                virtualCluster.getFilters(),
                endpointReconciler,
                new ApiVersionsServiceImpl(),
                DELEGATING_PREDICATE,
                new DefaultSubjectBuilder(List.of()),
                proxyChannelStateMachine,
                Optional.empty());

        TopicNameCacheFilter topicNameCacheFilter = new TopicNameCacheFilter(CacheConfiguration.DEFAULT, CLUSTER_NAME);
        when(virtualCluster.getTopicNameCacheFilter()).thenReturn(topicNameCacheFilter);

    }

    @Test
    void channelActive() throws Exception {
        // Given

        // When
        handler.channelActive(clientCtx);

        // Then
        verify(proxyChannelStateMachine).onClientActive(handler);
    }

    @Test
    void channelRead() {
        // Given
        HAProxyMessage msg = new HAProxyMessage(
                HAProxyProtocolVersion.V2,
                HAProxyCommand.PROXY,
                HAProxyProxiedProtocol.TCP4,
                "1.1.1.1",
                "2.2.2.2",
                1234,
                4567);

        // When
        handler.channelRead(clientCtx, msg);

        // Then
        verify(proxyChannelStateMachine).onClientRequest(msg);
    }

    @Test
    void shouldNotifyStateMachineWhenChannelBecomesUnWriteable() throws Exception {
        // Given
        handler.channelActive(clientCtx);
        final Channel channel = mock(Channel.class);
        when(clientCtx.channel()).thenReturn(channel);
        when(channel.isWritable()).thenReturn(false);

        // When
        handler.channelWritabilityChanged(clientCtx);

        // Then
        verify(proxyChannelStateMachine).onClientUnwritable();
    }

    @Test
    void shouldNotifyStateMachineWhenChannelBecomesWriteable() throws Exception {
        // Given
        handler.channelActive(clientCtx);
        final Channel channel = mock(Channel.class);
        when(clientCtx.channel()).thenReturn(channel);
        when(channel.isWritable()).thenReturn(true);

        // When
        handler.channelWritabilityChanged(clientCtx);

        // Then
        verify(proxyChannelStateMachine).onClientWritable();
    }

    @Test
    void shouldRemovePreSessionIdleHandlerWhenSessionSaslAuthenticated() throws Exception {
        // Given
        handler.channelActive(clientCtx);
        when(clientCtx.pipeline()).thenReturn(channelPipeline);
        ChannelHandler idleHandler = mock(ChannelHandler.class);
        when(channelPipeline.get(KafkaProxyInitializer.PRE_SESSION_IDLE_HANDLER))
                .thenReturn(idleHandler);

        // When
        handler.onSessionAuthenticated();

        // Then
        verify(channelPipeline).remove(idleHandler);
    }

    @Test
    void shouldHandleReAuthentication() throws Exception {
        // Given
        handler.channelActive(clientCtx);
        when(clientCtx.pipeline()).thenReturn(channelPipeline);
        ChannelHandler idleHandler = mock(ChannelHandler.class);
        when(channelPipeline.get(KafkaProxyInitializer.PRE_SESSION_IDLE_HANDLER))
                .thenReturn(idleHandler)
                .thenReturn(null);
        when(channelPipeline.remove(idleHandler)).thenReturn(channelPipeline);

        // not strictly required for the test, but it mimics the netty API. It also ensures that an erroneous call to remove will fail
        when(channelPipeline.remove((ChannelHandler) null)).thenThrow(new NullPointerException("handler"));

        handler.onSessionAuthenticated();

        // When
        handler.onSessionAuthenticated();

        // Then
        verify(channelPipeline, times(1)).remove(idleHandler);
    }

    @Test
    void shouldNotAddAuthenticatedSessionIdleHandlerWhenSessionSaslAuthenticated() throws Exception {
        // Given
        handler.channelActive(clientCtx);
        when(clientCtx.pipeline()).thenReturn(channelPipeline);
        ChannelHandler idleHandler = mock(ChannelHandler.class);
        when(channelPipeline.get(KafkaProxyInitializer.PRE_SESSION_IDLE_HANDLER))
                .thenReturn(idleHandler);

        // When
        handler.onSessionAuthenticated();

        // Then

        verify(channelPipeline, never()).addFirst(eq("authenticatedSessionIdleHandler"), any());
    }

    @Test
    void shouldAddAuthenticatedSessionIdleHandlerWithConfiguredTimeoutsWhenSessionSaslAuthenticated() throws Exception {
        // Given
        handler = new KafkaProxyFrontendHandler(
                mock(PluginFactoryRegistry.class),
                mock(FilterChainFactory.class),
                List.of(),
                endpointReconciler,
                mock(ApiVersionsServiceImpl.class),
                DELEGATING_PREDICATE,
                new DefaultSubjectBuilder(List.of()),
                proxyChannelStateMachine,
                Optional.of(NETTY_SETTINGS));
        handler.channelActive(clientCtx);
        when(clientCtx.pipeline()).thenReturn(channelPipeline);
        ChannelHandler idleHandler = mock(ChannelHandler.class);
        when(channelPipeline.get(KafkaProxyInitializer.PRE_SESSION_IDLE_HANDLER))
                .thenReturn(idleHandler);
        ArgumentCaptor<? extends ChannelHandler> handlerCaptor = ArgumentCaptor.forClass(ChannelHandler.class);

        // When
        handler.onSessionAuthenticated();

        // Then
        verify(channelPipeline).addFirst(eq("authenticatedSessionIdleHandler"), handlerCaptor.capture());
        assertThat(handlerCaptor.getValue())
                .isInstanceOfSatisfying(IdleStateHandler.class,
                        idleStateHandler -> assertThat(idleStateHandler.getAllIdleTimeInMillis()).isEqualTo(33_000L));
    }

    @Test
    void shouldOnlyAddAuthenticatedSessionIdleHandlerOnce() throws Exception {
        // Given
        handler = new KafkaProxyFrontendHandler(
                mock(PluginFactoryRegistry.class),
                mock(FilterChainFactory.class),
                List.of(),
                endpointReconciler,
                mock(ApiVersionsServiceImpl.class),
                DELEGATING_PREDICATE,
                new DefaultSubjectBuilder(List.of()),
                proxyChannelStateMachine,
                Optional.of(NETTY_SETTINGS));
        handler.channelActive(clientCtx);
        when(clientCtx.pipeline()).thenReturn(channelPipeline);
        ChannelHandler idleHandler = mock(ChannelHandler.class);
        when(channelPipeline.get(KafkaProxyInitializer.PRE_SESSION_IDLE_HANDLER)).thenReturn(idleHandler);
        when(channelPipeline.names()).thenReturn(List.of()).thenReturn(List.of("authenticatedSessionIdleHandler"));
        ArgumentCaptor<? extends ChannelHandler> handlerCaptor = ArgumentCaptor.forClass(ChannelHandler.class);
        handler.onSessionAuthenticated();

        // When
        handler.onSessionAuthenticated();

        // Then
        verify(channelPipeline, times(1)).addFirst(eq("authenticatedSessionIdleHandler"), handlerCaptor.capture());
    }

    @Test
    void shouldMarkSessionAuthenticatedWhenSessionTransportAuthenticated() throws Exception {
        // Given
        TransportSubjectBuilder subjectBuilder = mock(TransportSubjectBuilder.class);
        Subject subject = new Subject(new User("bob"));
        when(subjectBuilder.buildTransportSubject(any())).thenReturn(CompletableFuture.completedStage(subject));
        handler = new KafkaProxyFrontendHandler(
                pfr,
                filterChainFactory,
                virtualCluster.getFilters(),
                endpointReconciler,
                new ApiVersionsServiceImpl(),
                DELEGATING_PREDICATE,
                subjectBuilder,
                proxyChannelStateMachine,
                Optional.empty());

        handler.channelActive(clientCtx);
        when(clientCtx.pipeline()).thenReturn(channelPipeline);
        ChannelHandler idleHandler = mock(ChannelHandler.class);
        when(channelPipeline.get(KafkaProxyInitializer.PRE_SESSION_IDLE_HANDLER))
                .thenReturn(idleHandler);

        // When
        handler.inClientActive();

        // Then
        verify(proxyChannelStateMachine).onSessionTransportAuthenticated();
    }

    @Test
    void shouldNotMarkSessionAuthenticatedWhenSessionTransportAuthenticatedIsAnonymous() throws Exception {
        // Given
        handler.channelActive(clientCtx);
        when(clientCtx.pipeline()).thenReturn(channelPipeline);
        ChannelHandler idleHandler = mock(ChannelHandler.class);
        when(channelPipeline.get(KafkaProxyInitializer.PRE_SESSION_IDLE_HANDLER))
                .thenReturn(idleHandler);

        // When
        handler.inClientActive(); // Drive the transition through inClientActive rather than onTransportSubjectBuilt so the state is initialised

        // Then
        verify(proxyChannelStateMachine, never()).onSessionTransportAuthenticated();
    }
}
