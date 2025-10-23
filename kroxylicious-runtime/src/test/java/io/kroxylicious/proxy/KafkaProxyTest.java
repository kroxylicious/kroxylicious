/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.incubator.channel.uring.IOUring;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringServerSocketChannel;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaProxyTest {

    @Test
    void shouldFailToStartIfRequireFilterConfigIsMissing() throws Exception {
        var config = """
                   virtualClusters:
                     - name: demo1
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: cluster1:9192
                   filterDefinitions:
                   - name: filter1
                     type: RequiresConfigFactory
                   defaultFilters:
                   - filter1
                """;
        var configParser = new ConfigParser();
        try (var kafkaProxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            assertThatThrownBy(kafkaProxy::startup)
                    .isInstanceOf(PluginConfigurationException.class)
                    .hasMessageContaining("Exception initializing filter factory filter1 with config null");
        }
    }

    static Stream<Arguments> detectsConflictingPorts() {
        return Stream.of(Arguments.of("bootstrap port conflict", """
                virtualClusters:
                  - name: demo1
                    targetCluster:
                      bootstrapServers: kafka.example:1234
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: localhost:9192
                  - name: demo2
                    targetCluster:
                      bootstrapServers: kafka.example:1234
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: localhost:9192 # Conflict
                """,
                "exclusive TCP bind of <any>:9192 for gateway 'default' of virtual cluster 'demo1' conflicts with exclusive TCP bind of <any>:9192 for gateway 'default' of virtual cluster 'demo2': exclusive port collision"),
                Arguments.of("broker port conflict", """
                        virtualClusters:
                          - name: demo1
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                            gateways:
                            - name: default
                              portIdentifiesNode:
                                bootstrapAddress: localhost:9192
                                nodeStartPort: 9193
                          - name: demo2
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                            gateways:
                            - name: default
                              portIdentifiesNode:
                                bootstrapAddress: localhost:8192
                                nodeStartPort: 9193 # Conflict
                        """,
                        "exclusive TCP bind of <any>:9193 for gateway 'default' of virtual cluster 'demo1' conflicts with exclusive TCP bind of <any>:9193 for gateway 'default' of virtual cluster 'demo2': exclusive port collision"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void detectsConflictingPorts(String name, String config, String expectedMessage) throws Exception {
        ConfigParser configParser = new ConfigParser();
        try (var kafkaProxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            assertThatThrownBy(kafkaProxy::startup).hasMessageContaining(expectedMessage);
        }
    }

    static Stream<Arguments> missingTls() {
        return Stream.of(Arguments.of("tls mismatch", """
                virtualClusters:
                  - name: demo1
                    gateways:
                    - name: default
                      sniHostIdentifiesNode:
                        bootstrapAddress: localhost:8192
                        advertisedBrokerAddressPattern: broker-$(nodeId)
                    targetCluster:
                      bootstrapServers: kafka.example:1234
                """,
                "When using 'sniHostIdentifiesNode', 'tls' must be provided (virtual cluster listener default)"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void missingTls(String name, String config, String expectedMessage) {
        ConfigParser configParser = new ConfigParser();

        assertThatThrownBy(() -> configParser.parseConfiguration(config))
                .hasStackTraceContaining(expectedMessage);
    }

    public static Stream<Arguments> parametersNonNullable() {
        return Stream.of(Arguments.of(null, Mockito.mock(Configuration.class), Features.defaultFeatures()),
                Arguments.of(Mockito.mock(PluginFactoryRegistry.class), null, Features.defaultFeatures()),
                Arguments.of(Mockito.mock(PluginFactoryRegistry.class), Mockito.mock(Configuration.class), null));
    }

    @ParameterizedTest
    @MethodSource
    @SuppressWarnings("resource")
    void parametersNonNullable(PluginFactoryRegistry pfr, Configuration config, Features features) {
        assertThatThrownBy(() -> new KafkaProxy(pfr, config, features)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void invalidConfigurationForFeatures() {
        var config = """
                   virtualClusters:
                     - name: demo1
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: cluster1:9192
                   development:
                     a: b
                """;

        var configuration = new ConfigParser().parseConfiguration(config);
        Features features = Features.defaultFeatures();
        assertThatThrownBy(() -> KafkaProxy.validate(configuration, features))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessage("invalid configuration: test-only configuration for proxy present, but loading test-only configuration not enabled");
    }

    @Test
    void supportsLivezEndpoint() throws Exception {
        var config = """
                   management:
                    port: 9190
                   virtualClusters:
                     - name: demo1
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: localhost:9192
                """;
        var configParser = new ConfigParser();
        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            proxy.startup();

            var client = HttpClient.newHttpClient();
            var uri = URI.create("http://localhost:9190/livez");
            var response = client.send(HttpRequest.newBuilder(uri).GET().build(), HttpResponse.BodyHandlers.discarding());
            assertThat(response.statusCode()).isEqualTo(200);
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldDefaultManagementThreadCountWhenNoNetworkNodePresent() throws Exception {
        var config = """
                   management:
                    port: 9190
                   virtualClusters:
                     - name: demo1
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: localhost:9192
                """;
        var configParser = new ConfigParser();
        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            proxy.startup();

            assertThat(proxy.managementEventGroup()).satisfies(eventGroupConfig -> assertThat(eventGroupConfig.workerGroup().iterator()).toIterable().hasSize(Runtime.getRuntime()
                    .availableProcessors()) );
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldDefaultManagementThreadCountWhenNetworkNodePresentWithoutManagementSettings() throws Exception {
        var config = """
                   management:
                    port: 9190
                   network:
                    proxy:
                      workerThreadCount: 2
                   virtualClusters:
                     - name: demo1
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: localhost:9192
                """;
        var configParser = new ConfigParser();
        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            proxy.startup();

            assertThat(proxy.managementEventGroup()).satisfies(eventGroupConfig -> assertThat(eventGroupConfig.workerGroup().iterator()).toIterable().hasSize(Runtime.getRuntime()
                    .availableProcessors()) );
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldUseConfiguredManagementThreadCount() throws Exception {
        var config = """
                   management:
                    port: 9190
                   network:
                    management:
                      workerThreadCount: 2
                   virtualClusters:
                     - name: demo1
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: localhost:9192
                """;
        var configParser = new ConfigParser();
        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            proxy.startup();

            assertThat(proxy.managementEventGroup()).satisfies(eventGroupConfig -> assertThat(eventGroupConfig.workerGroup().iterator()).toIterable().hasSize(2) );
        }
    }

    @Nested
    @DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
    class EventGroupConfigTest {

        @Test
        void build_whenIoUringIsConfiguredToBeUsedAndAvailable_shouldUseIoUring() {
            // the constructor is mocked since native classes used in actual constructors can be unavailable based on the test infra
            try (var mockIOUring = Mockito.mockStatic(IOUring.class);
                    var mockGroupConstructor = Mockito.mockConstruction(IOUringEventLoopGroup.class)) {
                mockIOUring.when(IOUring::isAvailable).thenReturn(true);
                final var config = KafkaProxy.EventGroupConfig.build("test", 1, true);
                assertThat(config.bossGroup()).isInstanceOf(IOUringEventLoopGroup.class);
                assertThat(config.workerGroup()).isInstanceOf(IOUringEventLoopGroup.class);
                assertThat(config.clazz()).isEqualTo(IOUringServerSocketChannel.class);
                assertThat(mockGroupConstructor.constructed()).hasSize(2);
            }
        }

        @Test
        void build_whenIoUringIsConfiguredToBeUsedAndNotAvailable_shouldThrowException() {
            try (var mockIOUring = Mockito.mockStatic(IOUring.class)) {
                mockIOUring.when(IOUring::isAvailable).thenReturn(false);
                // noinspection ResultOfMethodCallIgnored
                mockIOUring.when(IOUring::unavailabilityCause).thenReturn(new Throwable());
                assertThatThrownBy(() -> KafkaProxy.EventGroupConfig.build("test", 1, true)).isInstanceOf(IllegalStateException.class);
            }
        }

        @Test
        void build_whenEpollIsAvailable_shouldUseEpoll() {
            try (var mockEpoll = Mockito.mockStatic(Epoll.class);
                    var mockGroupConstructor = Mockito.mockConstruction(EpollEventLoopGroup.class)) {
                mockEpoll.when(Epoll::isAvailable).thenReturn(true);
                final var config = KafkaProxy.EventGroupConfig.build("test", 1, false);
                assertThat(config.bossGroup()).isInstanceOf(EpollEventLoopGroup.class);
                assertThat(config.workerGroup()).isInstanceOf(EpollEventLoopGroup.class);
                assertThat(config.clazz()).isEqualTo(EpollServerSocketChannel.class);
                assertThat(mockGroupConstructor.constructed()).hasSize(2);
            }
        }

        @Test
        void build_whenEpollIsUnavailableAndKQueueIsAvailable_shouldUseKQueue() {
            try (var mockEpoll = Mockito.mockStatic(Epoll.class);
                    var mockKQueue = Mockito.mockStatic(KQueue.class);
                    var mockGroupConstructor = Mockito.mockConstruction(KQueueEventLoopGroup.class)) {
                mockEpoll.when(Epoll::isAvailable).thenReturn(false);
                mockKQueue.when(KQueue::isAvailable).thenReturn(true);
                final var config = KafkaProxy.EventGroupConfig.build("test", 1, false);
                assertThat(config.bossGroup()).isInstanceOf(KQueueEventLoopGroup.class);
                assertThat(config.workerGroup()).isInstanceOf(KQueueEventLoopGroup.class);
                assertThat(config.clazz()).isEqualTo(KQueueServerSocketChannel.class);
                assertThat(mockGroupConstructor.constructed()).hasSize(2);
            }
        }

        @Test
        void build_whenEpollAndKqueueAreUnavailable_shouldFallbackToNio() {
            try (var mockEpoll = Mockito.mockStatic(Epoll.class);
                    var mockKQueue = Mockito.mockStatic(KQueue.class);
                    var mockGroupConstructor = Mockito.mockConstruction(NioEventLoopGroup.class)) {
                mockEpoll.when(Epoll::isAvailable).thenReturn(false);
                mockKQueue.when(KQueue::isAvailable).thenReturn(false);
                final var config = KafkaProxy.EventGroupConfig.build("test", 1, false);
                assertThat(config.bossGroup()).isInstanceOf(NioEventLoopGroup.class);
                assertThat(config.workerGroup()).isInstanceOf(NioEventLoopGroup.class);
                assertThat(config.clazz()).isEqualTo(NioServerSocketChannel.class);
                assertThat(mockGroupConstructor.constructed()).hasSize(2);
            }
        }
    }
}
