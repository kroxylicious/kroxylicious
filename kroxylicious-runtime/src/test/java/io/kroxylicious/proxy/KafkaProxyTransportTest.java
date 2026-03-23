/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.RestoreSystemProperties;
import org.junitpioneer.jupiter.SetSystemProperty;
import org.mockito.Mockito;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.uring.IoUring;
import io.netty.channel.uring.IoUringServerSocketChannel;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.NettySettings;
import io.kroxylicious.proxy.config.NetworkDefinition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
// prevents Netty actually trying to load the native libraries, in a static initializer block, so we can mock the responses on all platforms
@SetSystemProperty(key = "io.netty.transport.noNative", value = "true")
@RestoreSystemProperties
@Tag("requiresIsolatedJvm")
class KafkaProxyTransportTest {

    private static final String MINIMUM_VIABLE_CONFIG_YAML = """
               virtualClusters:
                 - name: demo1
                   targetCluster:
                     bootstrapServers: kafka.example:1234
                   gateways:
                   - name: default
                     portIdentifiesNode:
                       bootstrapAddress: localhost:9192
            """;
    private Configuration configuration;

    @BeforeEach
    void setUp() {
        configuration = new ConfigParser().parseConfiguration(MINIMUM_VIABLE_CONFIG_YAML);
    }

    @Test
    void build_whenIoUringIsConfiguredToBeUsedAndAvailable_shouldUseIoUring() {
        // Given
        // the constructor is mocked since native classes used in actual constructors can be unavailable based on the test infra
        try (var mockTransport = Mockito.mockStatic(IoUring.class); var mockGroupConstructor = Mockito.mockConstruction(MultiThreadIoEventLoopGroup.class)) {
            mockTransport.when(IoUring::isAvailable).thenReturn(true);

            // When
            final var config = KafkaProxy.EventGroupConfig.build("test", configuration, NetworkDefinition::proxy, true);

            // Then
            assertThat(config.clazz()).isEqualTo(IoUringServerSocketChannel.class);
            assertThat(mockGroupConstructor.constructed()).hasSize(2);
        }
    }

    @Test
    void build_whenIoUringIsConfiguredToBeUsedAndNotAvailable_shouldThrowException() {
        // Given
        try (var mockIOUring = Mockito.mockStatic(IoUring.class)) {
            mockIOUring.when(IoUring::isAvailable).thenReturn(false);
            // noinspection ResultOfMethodCallIgnored
            mockIOUring.when(IoUring::unavailabilityCause).thenReturn(new Throwable());
            assertThatThrownBy(() -> KafkaProxy.EventGroupConfig.build("test", configuration, NetworkDefinition::proxy, true))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void build_whenEpollIsAvailable_shouldUseEpoll() {
        // Given
        // the constructor is mocked since native classes used in actual constructors can be unavailable based on the test infra
        try (var mockTransport = Mockito.mockStatic(Epoll.class); var mockGroupConstructor = Mockito.mockConstruction(MultiThreadIoEventLoopGroup.class)) {
            mockTransport.when(Epoll::isAvailable).thenReturn(true);

            // When
            final var config = KafkaProxy.EventGroupConfig.build("test", configuration, NetworkDefinition::proxy, false);

            // Then
            assertThat(config.clazz()).isEqualTo(EpollServerSocketChannel.class);
            assertThat(mockGroupConstructor.constructed()).hasSize(2);
        }
    }

    @Test
    void build_whenEpollIsUnavailableAndKQueueIsAvailable_shouldUseKQueue() {
        // Given
        // the constructor is mocked since native classes used in actual constructors can be unavailable based on the test infra
        try (var kQueueTransport = Mockito.mockStatic(KQueue.class);
                var epollTransport = Mockito.mockStatic(Epoll.class);
                var mockGroupConstructor = Mockito.mockConstruction(MultiThreadIoEventLoopGroup.class)) {
            epollTransport.when(Epoll::isAvailable).thenReturn(false);
            kQueueTransport.when(KQueue::isAvailable).thenReturn(true);

            // When
            final var config = KafkaProxy.EventGroupConfig.build("test", configuration, NetworkDefinition::proxy, false);

            // Then
            assertThat(config.clazz()).isEqualTo(KQueueServerSocketChannel.class);
            assertThat(mockGroupConstructor.constructed()).hasSize(2);
        }
    }

    @Test
    void build_shouldFallbackToNio() {
        try (var mockEpoll = Mockito.mockStatic(Epoll.class);
                var mockKQueue = Mockito.mockStatic(KQueue.class);
                var mockGroupConstructor = Mockito.mockConstruction(MultiThreadIoEventLoopGroup.class)) {
            mockEpoll.when(Epoll::isAvailable).thenReturn(false);
            mockKQueue.when(KQueue::isAvailable).thenReturn(false);

            final var config = KafkaProxy.EventGroupConfig.build("test", configuration, NetworkDefinition::proxy, false);
            assertThat(config.clazz()).isEqualTo(NioServerSocketChannel.class);
            assertThat(mockGroupConstructor.constructed()).hasSize(2);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void shutdownGracefully_shouldUseDefaultQuietPeriodAndTimeout_whenNettySettingsAbsent() {
        var eventGroupConfig = eventGroupConfigWithMockedGroups();

        eventGroupConfig.shutdownGracefully(Optional.empty());

        verifyShutdownGracefully(eventGroupConfig, Duration.ofSeconds(2), Duration.ofSeconds(15));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shutdownGracefully_shouldUseShutdownQuietPeriod_whenSet() {
        // sub-second value proves no truncation to whole seconds
        var settings = new NettySettings(Optional.empty(), Optional.empty(), Optional.of(Duration.ofMillis(500)), Optional.empty(), Optional.empty(), Optional.empty());
        var eventGroupConfig = eventGroupConfigWithMockedGroups();

        eventGroupConfig.shutdownGracefully(Optional.of(settings));

        verifyShutdownGracefully(eventGroupConfig, Duration.ofMillis(500), Duration.ofSeconds(15));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shutdownGracefully_shouldUseShutdownTimeout_whenSet() {
        var settings = new NettySettings(Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(Duration.ofSeconds(45)), Optional.empty(), Optional.empty());
        var eventGroupConfig = eventGroupConfigWithMockedGroups();

        eventGroupConfig.shutdownGracefully(Optional.of(settings));

        verifyShutdownGracefully(eventGroupConfig, Duration.ofSeconds(2), Duration.ofSeconds(45));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shutdownGracefully_shouldFallBackToDeprecatedShutdownQuietPeriodSeconds_whenShutdownQuietPeriodNotSet() {
        var settings = new NettySettings(Optional.empty(), Optional.of(7), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        var eventGroupConfig = eventGroupConfigWithMockedGroups();

        eventGroupConfig.shutdownGracefully(Optional.of(settings));

        verifyShutdownGracefully(eventGroupConfig, Duration.ofSeconds(7), Duration.ofSeconds(15));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shutdownGracefully_shouldPreferShutdownQuietPeriod_overDeprecatedShutdownQuietPeriodSeconds() {
        var settings = new NettySettings(Optional.empty(), Optional.of(7), Optional.of(Duration.ofMillis(500)), Optional.empty(), Optional.empty(), Optional.empty());
        var eventGroupConfig = eventGroupConfigWithMockedGroups();

        eventGroupConfig.shutdownGracefully(Optional.of(settings));

        verifyShutdownGracefully(eventGroupConfig, Duration.ofMillis(500), Duration.ofSeconds(15));
    }

    @SuppressWarnings("unchecked")
    private KafkaProxy.EventGroupConfig eventGroupConfigWithMockedGroups() {
        var future = Mockito.mock(io.netty.util.concurrent.Future.class);
        var bossGroup = Mockito.mock(EventLoopGroup.class);
        var workerGroup = Mockito.mock(EventLoopGroup.class);
        Mockito.when(bossGroup.shutdownGracefully(Mockito.anyLong(), Mockito.anyLong(), Mockito.any())).thenReturn(future);
        Mockito.when(workerGroup.shutdownGracefully(Mockito.anyLong(), Mockito.anyLong(), Mockito.any())).thenReturn(future);
        return new KafkaProxy.EventGroupConfig("test", bossGroup, workerGroup, NioServerSocketChannel.class);
    }

    private static void verifyShutdownGracefully(KafkaProxy.EventGroupConfig eventGroupConfig, Duration quietPeriod, Duration timeout) {
        Mockito.verify(eventGroupConfig.bossGroup()).shutdownGracefully(quietPeriod.toNanos(), timeout.toNanos(), TimeUnit.NANOSECONDS);
        Mockito.verify(eventGroupConfig.workerGroup()).shutdownGracefully(quietPeriod.toNanos(), timeout.toNanos(), TimeUnit.NANOSECONDS);
    }
}
