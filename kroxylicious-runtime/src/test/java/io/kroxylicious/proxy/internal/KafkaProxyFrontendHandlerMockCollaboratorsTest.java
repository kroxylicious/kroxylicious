/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

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

import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NettySettings;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.internal.filter.ApiVersionsDowngradeFilter;
import io.kroxylicious.proxy.internal.filter.ApiVersionsIntersectFilter;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.internal.subject.DefaultSubjectBuilder;
import io.kroxylicious.proxy.model.VirtualClusterModel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaProxyFrontendHandlerMockCollaboratorsTest {

    public static final String SOURCE_ADDRESS = "1.1.1.1";
    public static final int SOURCE_PORT = 18466;
    public static final HAProxyMessage HA_PROXY_MESSAGE = new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
            SOURCE_ADDRESS, "1.0.0.1", SOURCE_PORT, 9090);
    public static final DelegatingDecodePredicate DELEGATING_PREDICATE = new DelegatingDecodePredicate();
    public static final NettySettings NETTY_SETTINGS = new NettySettings(Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(Duration.ofSeconds(33)));

    @Mock(strictness = Mock.Strictness.LENIENT)
    private ChannelPipeline channelPipeline;

    @Mock
    PluginFactoryRegistry pfr;
    @Mock
    Channel ch;
    @Mock
    FilterChainFactory filterChainFactory;
    @Mock
    List<NamedFilterDefinition> filters;
    @Mock
    EndpointReconciler endpointReconciler;
    @Mock
    ApiVersionsIntersectFilter apiVersionsIntersectFilter;
    @Mock
    ApiVersionsDowngradeFilter apiVersionsDowngradeFilter;

    @Mock
    VirtualClusterModel virtualCluster;

    @Mock
    EndpointBinding endpointBinding;

    @Mock
    EndpointGateway endpointGateway;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    ChannelHandlerContext clientCtx;

    @Mock
    ProxyChannelStateMachine proxyChannelStateMachine;
    private KafkaProxyFrontendHandler handler;

    @BeforeEach
    void setUp() {
        when(endpointGateway.virtualCluster()).thenReturn(virtualCluster);
        when(endpointBinding.endpointGateway()).thenReturn(endpointGateway);
        handler = new KafkaProxyFrontendHandler(
                pfr, filterChainFactory, virtualCluster.getFilters(), endpointReconciler, new ApiVersionsServiceImpl(), DELEGATING_PREDICATE,
                new DefaultSubjectBuilder(List.of()),
                endpointBinding,
                proxyChannelStateMachine,
                Optional.empty());
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
    void shouldRemovePreSessionIdleHandlerWhenSessionAuthenticated() throws Exception {
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
    void shouldAddAuthenticatedSessionIdleHandlerWithDefaultTimeoutsWhenSessionAuthenticated() throws Exception {
        // Given
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
                        idleStateHandler -> assertThat(idleStateHandler.getAllIdleTimeInMillis()).isEqualTo(31_000L));
    }

    @Test
    void shouldAddAuthenticatedSessionIdleHandlerWithConfiguredTimeoutsWhenSessionAuthenticated() throws Exception {
        // Given
        handler = new KafkaProxyFrontendHandler(
                mock(PluginFactoryRegistry.class),
                mock(FilterChainFactory.class),
                List.of(),
                endpointReconciler,
                mock(ApiVersionsServiceImpl.class),
                DELEGATING_PREDICATE,
                new DefaultSubjectBuilder(List.of()),
                endpointBinding,
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

}
