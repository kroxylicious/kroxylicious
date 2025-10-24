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

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.RestoreSystemProperties;
import org.junitpioneer.jupiter.SetSystemProperty;
import org.mockito.Mockito;

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
import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.config.NetworkDefinition;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaProxyTest {

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
    private ConfigParser configParser;

    @BeforeEach
    void setUp() {
        configParser = new ConfigParser();
    }

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
        try (var kafkaProxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            assertThatThrownBy(kafkaProxy::startup)
                    .isInstanceOf(PluginConfigurationException.class)
                    .hasMessageContaining("Exception initializing filter factory filter1 with config null");
        }
    }

    static Stream<Arguments> detectsConflictingPorts() {
        return Stream.of(Arguments.argumentSet("bootstrap port conflict", """
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
                Arguments.argumentSet("broker port conflict", """
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

    @ParameterizedTest()
    @MethodSource
    void detectsConflictingPorts(String config, String expectedMessage) throws Exception {
        try (var kafkaProxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            assertThatThrownBy(kafkaProxy::startup).hasMessageContaining(expectedMessage);
        }
    }

    static Stream<Arguments> missingTls() {
        return Stream.of(Arguments.argumentSet("tls mismatch", """
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

    @ParameterizedTest
    @MethodSource
    void missingTls(String config, String expectedMessage) {
        assertThatThrownBy(() -> configParser.parseConfiguration(config))
                .hasStackTraceContaining(expectedMessage);
    }

    public static Stream<Arguments> parametersNonNullable() {
        return Stream.of(Arguments.argumentSet("Null registry", null, Mockito.mock(Configuration.class), Features.defaultFeatures()),
                Arguments.argumentSet("null config", Mockito.mock(PluginFactoryRegistry.class), null, Features.defaultFeatures()),
                Arguments.argumentSet("null features", Mockito.mock(PluginFactoryRegistry.class), Mockito.mock(Configuration.class), null));
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

        var configuration = configParser.parseConfiguration(config);
        Features features = Features.defaultFeatures();
        assertThatThrownBy(() -> KafkaProxy.validate(configuration, features))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessage("invalid configuration: test-only configuration for proxy present, but loading test-only configuration not enabled");
    }

    @Test
    void supportsLivezEndpoint() throws Exception {
        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration("""
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
                """), Features.defaultFeatures())) {
            proxy.startup();
            @SuppressWarnings("resource") // it's not auto closable in java 17
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
        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            proxy.startup();

            assertThat(proxy.managementEventGroup())
                    .satisfies(eventGroupConfig -> assertThat(eventGroupConfig.workerGroup().iterator()).toIterable()
                            .hasSize(Runtime.getRuntime().availableProcessors()));
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
        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            proxy.startup();

            assertThat(proxy.managementEventGroup())
                    .satisfies(eventGroupConfig -> assertThat(eventGroupConfig.workerGroup().iterator()).toIterable()
                            .hasSize(Runtime.getRuntime().availableProcessors()));
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
        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            proxy.startup();

            assertThat(proxy.managementEventGroup()).satisfies(eventGroupConfig -> assertThat(eventGroupConfig.workerGroup().iterator()).toIterable().hasSize(2));
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldDefaultProxyThreadCountWhenNoNetworkNodePresent() throws Exception {
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
        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            proxy.startup();

            assertThat(proxy.proxyEventGroup())
                    .satisfies(eventGroupConfig -> assertThat(eventGroupConfig.workerGroup().iterator()).toIterable()
                            .hasSize(Runtime.getRuntime().availableProcessors()));
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldDefaultProxyThreadCountWhenNetworkNodePresentWithoutProxySettings() throws Exception {
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
        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            proxy.startup();

            assertThat(proxy.proxyEventGroup())
                    .satisfies(eventGroupConfig -> assertThat(eventGroupConfig.workerGroup().iterator()).toIterable()
                            .hasSize(Runtime.getRuntime().availableProcessors()));
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldUseConfiguredProxyThreadCount() throws Exception {
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
        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            proxy.startup();

            assertThat(proxy.proxyEventGroup()).satisfies(eventGroupConfig -> assertThat(eventGroupConfig.workerGroup().iterator()).toIterable().hasSize(2));
        }
    }

    @Test
    void shouldNotAllowMultipleConcurrentStarts() throws Exception {
        var configParser = new ConfigParser();
        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(MINIMUM_VIABLE_CONFIG_YAML), Features.defaultFeatures())) {
            proxy.startup();

            assertThatThrownBy(proxy::startup).isInstanceOf(IllegalStateException.class).hasMessage("This proxy is already running");
        }
    }

    @Test
    void shouldNotAllowShuttingDownOfAStoppedInstance() throws Exception {
        var configParser = new ConfigParser();
        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(MINIMUM_VIABLE_CONFIG_YAML), Features.defaultFeatures())) {
            assertThatThrownBy(proxy::shutdown).isInstanceOf(IllegalStateException.class).hasMessage("This proxy is not running");
        }
    }

    @Test
    @EnabledIf(value = "io.netty.channel.uring.IoUring#isAvailable", disabledReason = "IOUring is not available")
    void shouldEnableIOUring() throws Exception {
        // Given
        var configParser = new ConfigParser();
        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration("""
                   useIoUring: true
                   virtualClusters:
                     - name: demo1
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: localhost:9192
                """), Features.defaultFeatures())) {
            // When
            KafkaProxy kafkaProxy = proxy.startup();

            // Then
            assertThat(kafkaProxy).isInstanceOf(KafkaProxy.class)
                    .extracting("managementEventGroup", InstanceOfAssertFactories.type(KafkaProxy.EventGroupConfig.class))
                    .satisfies(eventGroupConfig -> assertThat(eventGroupConfig.clazz()).isAssignableFrom(IoUringServerSocketChannel.class));

        }
    }

    @Test
    void shouldFallbackIfIOUringDisabled() throws Exception {
        // Given
        var configParser = new ConfigParser();
        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration("""
                   useIoUring: false
                   virtualClusters:
                     - name: demo1
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: localhost:9192
                """), Features.defaultFeatures())) {
            // When
            KafkaProxy kafkaProxy = proxy.startup();

            // Then
            assertThat(kafkaProxy).isInstanceOf(KafkaProxy.class)
                    .extracting("managementEventGroup", InstanceOfAssertFactories.type(KafkaProxy.EventGroupConfig.class))
                    .satisfiesAnyOf(eventGroupConfig -> assertThat(eventGroupConfig.clazz()).isAssignableFrom(EpollServerSocketChannel.class),
                            eventGroupConfig -> assertThat(eventGroupConfig.clazz()).isAssignableFrom(NioServerSocketChannel.class),
                            eventGroupConfig -> assertThat(eventGroupConfig.clazz()).isAssignableFrom(KQueueServerSocketChannel.class));

        }
    }

    @Test
    @DisabledIf(value = "io.netty.channel.uring.IoUring#isAvailable", disabledReason = "IOUring is available")
    void shouldFailToStartIfIouUringConfiguredAndUnavailable() throws Exception {
        // Given
        var configParser = new ConfigParser();
        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration("""
                   useIoUring: true
                   virtualClusters:
                     - name: demo1
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: localhost:9192
                """), Features.defaultFeatures())) {
            // When
            // Then
            assertThatThrownBy(proxy::startup).isInstanceOf(IllegalStateException.class).hasMessageStartingWith("io_uring not available due to: ");
        }
    }

    @Nested
    @DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
    @SetSystemProperty(key = "io.netty.transport.noNative", value = "true")
    @RestoreSystemProperties
    class EventGroupConfigTest {

        private Configuration configuration;

        @BeforeEach
        void setUp() {
            configuration = configParser.parseConfiguration(MINIMUM_VIABLE_CONFIG_YAML);
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
    }
}
