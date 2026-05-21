/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Serving;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Stopped;
import io.kroxylicious.proxy.internal.VirtualClusterRegistry;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaProxyLifecycleTest {

    // Zero quiet period avoids the 2-second default Netty shutdown wait in each test.
    private static final String DEMO1_CONFIG = """
            network:
              proxy:
                shutdownQuietPeriod: 0s
              management:
                shutdownQuietPeriod: 0s
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
    private KafkaProxy proxy;

    @BeforeEach
    void setUp() {
        configParser = new ConfigParser();
    }

    @AfterEach
    void tearDown() {
        if (this.proxy != null) {
            this.proxy.close();
        }
    }

    @Test
    void shouldTrackVirtualClusterAsServingAfterStartup() {
        // given
        var config = DEMO1_CONFIG;

        try (var kafkaProxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            // when
            kafkaProxy.startup();

            // then
            assertThat(kafkaProxy.lifecycleFor("demo1"))
                    .isNotNull()
                    .satisfies(m -> assertThat(m.state()).isInstanceOf(Serving.class));
        }
    }

    @Test
    void shouldTrackMultipleVirtualClustersAsServing() {
        // given
        var config = """
                network:
                  proxy:
                    shutdownQuietPeriod: 0s
                  management:
                    shutdownQuietPeriod: 0s
                virtualClusters:
                  - name: cluster-a
                    targetCluster:
                      bootstrapServers: kafka.example:1234
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: localhost:9192
                  - name: cluster-b
                    targetCluster:
                      bootstrapServers: kafka.example:5678
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: localhost:9292
                """;

        try (var kafkaProxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            // when
            kafkaProxy.startup();

            // then
            assertThat(kafkaProxy.lifecycleFor("cluster-a"))
                    .isNotNull()
                    .satisfies(m -> assertThat(m.state()).isInstanceOf(Serving.class));
            assertThat(kafkaProxy.lifecycleFor("cluster-b"))
                    .isNotNull()
                    .satisfies(m -> assertThat(m.state()).isInstanceOf(Serving.class));
        }
    }

    @Test
    void shouldTransitionToStoppedAfterShutdown() {
        this.proxy = new KafkaProxy(configParser, configParser.parseConfiguration(DEMO1_CONFIG), Features.defaultFeatures());
        proxy.startup();
        var manager = proxy.lifecycleFor("demo1");

        // when
        proxy.shutdown();

        // then
        assertThat(manager).isNotNull();
        assertThat(manager.state()).isInstanceOf(Stopped.class);
    }

    @Test
    void startupReturnsFutureThatCompletesOnShutdown() {
        this.proxy = new KafkaProxy(configParser, configParser.parseConfiguration(DEMO1_CONFIG), Features.defaultFeatures());
        CompletableFuture<Void> future = proxy.startup();
        assertThat(future).isNotNull().isNotDone();
        proxy.shutdown();
        assertThat(future).isCompletedWithValue(null);
    }

    @Test
    void startupTwiceReturnsSameFutureInstance() {
        this.proxy = new KafkaProxy(configParser, configParser.parseConfiguration(DEMO1_CONFIG), Features.defaultFeatures());
        try {
            CompletableFuture<Void> first = proxy.startup();
            CompletableFuture<Void> second = proxy.startup();
            assertThat(second).isSameAs(first);
        }
        finally {
            proxy.shutdown();
        }
    }

    @Test
    void shutdownBeforeStartupIsNoOp() {
        this.proxy = new KafkaProxy(configParser, configParser.parseConfiguration(DEMO1_CONFIG), Features.defaultFeatures());
        assertThatCode(proxy::shutdown).doesNotThrowAnyException();
    }

    @Test
    void shutdownTwiceIsNoOp() {
        this.proxy = new KafkaProxy(configParser, configParser.parseConfiguration(DEMO1_CONFIG), Features.defaultFeatures());
        proxy.startup();
        proxy.shutdown();
        assertThatCode(proxy::shutdown).doesNotThrowAnyException();
    }

    @Test
    void startupAfterStopThrowsIllegalState() {
        this.proxy = new KafkaProxy(configParser, configParser.parseConfiguration(DEMO1_CONFIG), Features.defaultFeatures());
        proxy.startup();
        proxy.shutdown();
        assertThatThrownBy(proxy::startup)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("KafkaProxy is not restartable");
    }

    @Test
    void cancelOnFutureTriggersGracefulShutdown() {
        this.proxy = new KafkaProxy(configParser, configParser.parseConfiguration(DEMO1_CONFIG), Features.defaultFeatures());
        CompletableFuture<Void> future = proxy.startup();

        boolean cancelled = future.cancel(true);

        assertThat(cancelled).isFalse();
        assertThat(future).isCompletedWithValue(null);
    }

    @Test
    void closeIsIdempotent() {
        this.proxy = new KafkaProxy(configParser, configParser.parseConfiguration(DEMO1_CONFIG), Features.defaultFeatures());
        proxy.startup();
        assertThatCode(proxy::close).doesNotThrowAnyException();
        assertThatCode(proxy::close).doesNotThrowAnyException();
    }

    @Test
    void shutdownFromDifferentThread() {
        this.proxy = new KafkaProxy(configParser, configParser.parseConfiguration(DEMO1_CONFIG), Features.defaultFeatures());
        CompletableFuture<Void> future = proxy.startup();

        CompletableFuture.runAsync(proxy::shutdown).join();

        assertThat(future).isCompletedWithValue(null);
    }

    @Test
    void futureCompletesExceptionallyWhenStartupFails() {
        var config = """
                network:
                  proxy:
                    shutdownQuietPeriod: 0s
                  management:
                    shutdownQuietPeriod: 0s
                virtualClusters:
                  - name: demo1
                    targetCluster:
                      bootstrapServers: kafka.example:1234
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: localhost:9192
                filterDefinitions:
                - name: filter1
                  type: RequiresConfigFactory
                defaultFilters:
                - filter1
                """;

        this.proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures());
        assertThatThrownBy(proxy::startup).isInstanceOf(LifecycleException.class);

        assertThat(proxy.shutdownFuture())
                .isCompletedExceptionally()
                .failsWithin(java.time.Duration.ZERO)
                .withThrowableOfType(java.util.concurrent.ExecutionException.class)
                .withCauseInstanceOf(LifecycleException.class);
    }

    @Test
    void startupWhileStoppingThrows() throws InterruptedException {
        var config = DEMO1_CONFIG;

        var configuration = configParser.parseConfiguration(config);
        var models = configuration.virtualClusterModel(configParser);

        var shutdownStarted = new CountDownLatch(1);
        var allowShutdown = new CountDownLatch(1);

        var blockingRegistry = new VirtualClusterRegistry(models, (name, cause) -> {
        }) {
            @Override
            public void shutdownAllClusters() {
                shutdownStarted.countDown();
                try {
                    allowShutdown.await();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                super.shutdownAllClusters();
            }
        };

        this.proxy = new KafkaProxy(configParser, configuration, Features.defaultFeatures(), blockingRegistry);
        proxy.startup();

        var shutdownThread = new Thread(proxy::shutdown);
        shutdownThread.start();
        shutdownStarted.await();

        try {
            // proxy is in STOPPING state while shutdownAllClusters() is blocked
            assertThatThrownBy(proxy::startup)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("KafkaProxy is not restartable");
        }
        finally {
            allowShutdown.countDown();
            shutdownThread.join(5_000);
        }
    }

    @Test
    void futureCompletesExceptionallyWhenShutdownFails() {
        var config = """
                network:
                  proxy:
                    shutdownQuietPeriod: 0s
                  management:
                    shutdownQuietPeriod: 0s
                virtualClusters:
                  - name: demo1
                    targetCluster:
                      bootstrapServers: kafka.example:1234
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: localhost:9192
                filterDefinitions:
                - name: filter1
                  type: FlakyFactory
                  config:
                    closeExceptionMsg: "simulated close failure"
                defaultFilters:
                - filter1
                """;

        this.proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures());
        CompletableFuture<Void> future = proxy.startup();

        proxy.shutdown();

        assertThat(future)
                .isCompletedExceptionally()
                .failsWithin(java.time.Duration.ZERO)
                .withThrowableOfType(java.util.concurrent.ExecutionException.class)
                .withCauseInstanceOf(RuntimeException.class)
                .withMessageContaining("simulated close failure");
    }

    @Test
    void shouldTransitionToStoppedOnStartupFailure() {
        // given
        var config = """
                network:
                  proxy:
                    shutdownQuietPeriod: 0s
                  management:
                    shutdownQuietPeriod: 0s
                virtualClusters:
                  - name: demo1
                    targetCluster:
                      bootstrapServers: kafka.example:1234
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: localhost:9192
                filterDefinitions:
                - name: filter1
                  type: RequiresConfigFactory
                defaultFilters:
                - filter1
                """;

        try (var kafkaProxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            // when
            assertThatThrownBy(kafkaProxy::startup)
                    .isInstanceOf(LifecycleException.class)
                    .cause()
                    .isInstanceOf(PluginConfigurationException.class);

            // then
            var manager = kafkaProxy.lifecycleFor("demo1");
            assertThat(manager).isNotNull();
            assertThat(manager.state())
                    .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.type(Stopped.class))
                    .extracting(Stopped::priorFailureCause)
                    .isInstanceOf(PluginConfigurationException.class);
        }
    }
}
