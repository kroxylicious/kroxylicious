/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.time.Duration;
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
    void build_shouldResolveDefaultShutdownDurations_whenNoNetworkConfig() {
        try (var mockGroupConstructor = Mockito.mockConstruction(MultiThreadIoEventLoopGroup.class)) {
            var config = KafkaProxy.EventGroupConfig.build("test", configuration, NetworkDefinition::proxy, false);
            assertThat(config.shutdownQuietPeriod()).isEqualTo(Duration.ofSeconds(2));
            assertThat(config.shutdownTimeout()).isEqualTo(Duration.ofSeconds(15));
        }
    }

    @Test
    void build_shouldResolveShutdownQuietPeriod_whenSet() {
        // sub-second proves no truncation to whole seconds
        var config = new ConfigParser().parseConfiguration(MINIMUM_VIABLE_CONFIG_YAML + """
                network:
                  proxy:
                    shutdownQuietPeriod: 500ms
                """);
        try (var mockGroupConstructor = Mockito.mockConstruction(MultiThreadIoEventLoopGroup.class)) {
            var eventGroupConfig = KafkaProxy.EventGroupConfig.build("test", config, NetworkDefinition::proxy, false);
            assertThat(eventGroupConfig.shutdownQuietPeriod()).isEqualTo(Duration.ofMillis(500));
        }
    }

    @Test
    void build_shouldResolveShutdownTimeout_whenSet() {
        var config = new ConfigParser().parseConfiguration(MINIMUM_VIABLE_CONFIG_YAML + """
                network:
                  proxy:
                    shutdownTimeout: 45s
                """);
        try (var mockGroupConstructor = Mockito.mockConstruction(MultiThreadIoEventLoopGroup.class)) {
            var eventGroupConfig = KafkaProxy.EventGroupConfig.build("test", config, NetworkDefinition::proxy, false);
            assertThat(eventGroupConfig.shutdownTimeout()).isEqualTo(Duration.ofSeconds(45));
        }
    }

    @Test
    void build_shouldFallBackToDeprecatedShutdownQuietPeriodSeconds_whenShutdownQuietPeriodNotSet() {
        var config = new ConfigParser().parseConfiguration(MINIMUM_VIABLE_CONFIG_YAML + """
                network:
                  proxy:
                    shutdownQuietPeriodSeconds: 7
                """);
        try (var mockGroupConstructor = Mockito.mockConstruction(MultiThreadIoEventLoopGroup.class)) {
            var eventGroupConfig = KafkaProxy.EventGroupConfig.build("test", config, NetworkDefinition::proxy, false);
            assertThat(eventGroupConfig.shutdownQuietPeriod()).isEqualTo(Duration.ofSeconds(7));
        }
    }

    @Test
    void build_shouldPreferShutdownQuietPeriod_overDeprecatedShutdownQuietPeriodSeconds() {
        var config = new ConfigParser().parseConfiguration(MINIMUM_VIABLE_CONFIG_YAML + """
                network:
                  proxy:
                    shutdownQuietPeriodSeconds: 7
                    shutdownQuietPeriod: 500ms
                """);
        try (var mockGroupConstructor = Mockito.mockConstruction(MultiThreadIoEventLoopGroup.class)) {
            var eventGroupConfig = KafkaProxy.EventGroupConfig.build("test", config, NetworkDefinition::proxy, false);
            assertThat(eventGroupConfig.shutdownQuietPeriod()).isEqualTo(Duration.ofMillis(500));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void shutdownGracefully_shouldCallNettyWithStoredDurationsAsNanos() {
        var future = Mockito.mock(io.netty.util.concurrent.Future.class);
        var bossGroup = Mockito.mock(EventLoopGroup.class);
        var workerGroup = Mockito.mock(EventLoopGroup.class);
        Mockito.when(bossGroup.shutdownGracefully(Mockito.anyLong(), Mockito.anyLong(), Mockito.any())).thenReturn(future);
        Mockito.when(workerGroup.shutdownGracefully(Mockito.anyLong(), Mockito.anyLong(), Mockito.any())).thenReturn(future);

        var quietPeriod = Duration.ofMillis(500);
        var timeout = Duration.ofSeconds(30);
        var eventGroupConfig = new KafkaProxy.EventGroupConfig("test", bossGroup, workerGroup, NioServerSocketChannel.class, quietPeriod, timeout);

        eventGroupConfig.shutdownGracefully();

        Mockito.verify(bossGroup).shutdownGracefully(quietPeriod.toNanos(), timeout.toNanos(), TimeUnit.NANOSECONDS);
        Mockito.verify(workerGroup).shutdownGracefully(quietPeriod.toNanos(), timeout.toNanos(), TimeUnit.NANOSECONDS);
    }
}
