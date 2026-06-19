/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
    void shutdownAfterReloadDrivesNewlyAddedVcToStopped() throws Exception {
        // Regression test for #4066: KafkaProxy.shutdown() must drive VCs added via reload to
        // Stopped, not just the ones present at proxy construction time. Prior to the FCF-per-VC
        // refactor, shutdown iterated a stale snapshot of virtualClusterModels captured at
        // construction time — any VC added via the reload API was invisible to shutdown.
        var initial = """
                   virtualClusters:
                     - name: vc-a
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: localhost:9492
                """;
        var afterReload = """
                   virtualClusters:
                     - name: vc-a
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: localhost:9492
                     - name: vc-b
                       targetCluster:
                         bootstrapServers: kafka.example:5678
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: localhost:9592
                """;

        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(initial), Features.defaultFeatures())) {
            proxy.startup();
            proxy.reconfigure(configParser.parseConfiguration(afterReload)).get(5, TimeUnit.SECONDS);

            var lifecycleA = proxy.lifecycleFor("vc-a");
            var lifecycleB = proxy.lifecycleFor("vc-b");
            assertThat(lifecycleB).as("vc-b should be tracked after reload").isNotNull();
            assertThat(lifecycleB.state()).as("vc-b should reach Serving after reload").isInstanceOf(Serving.class);

            // when
            proxy.shutdown();

            // then — BOTH the originally-configured vc-a AND the runtime-added vc-b must reach Stopped.
            assertThat(lifecycleA.state())
                    .as("originally-configured vc-a should reach Stopped on shutdown")
                    .isInstanceOf(Stopped.class);
            assertThat(lifecycleB.state())
                    .as("runtime-added vc-b should also reach Stopped on shutdown (regression test for #4066)")
                    .isInstanceOf(Stopped.class);
        }
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
    void startupWhileStoppingThrows() throws InterruptedException {
        var config = DEMO1_CONFIG;

        var configuration = configParser.parseConfiguration(config);
        var models = configuration.virtualClusterModel(configParser);

        var shutdownStarted = new CountDownLatch(1);
        var allowShutdown = new CountDownLatch(1);

        var blockingRegistry = new VirtualClusterRegistry(models, (cfg, clusterName) -> {
            throw new UnsupportedOperationException("resolveModel not exercised by this test");
        }, (name, cause) -> {
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
    void shutdownFutureCompletesNormallyWhenCloseThrows() {
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

        assertThat(future).isCompletedWithValue(null);
    }

    @Test
    void shouldCompleteShutdownWithConcurrentStartup() throws InterruptedException {
        // Deterministically reproduce the race where shutdown() is called after
        // initializationSucceeded() (VCs now Serving) but before transitionTo(STARTED).
        // The proxy is fully initialised at this point; startup() should return the
        // shutdown future without throwing, and the future should complete normally.
        var configuration = configParser.parseConfiguration(DEMO1_CONFIG);
        var models = configuration.virtualClusterModel(configParser);

        var initializationCompleted = new CountDownLatch(1);
        var allowStartupToComplete = new CountDownLatch(1);

        var blockingRegistry = new VirtualClusterRegistry(models, (cfg, clusterName) -> {
            throw new UnsupportedOperationException("resolveModel not exercised by this test");
        }, (name, cause) -> {
        }) {
            @Override
            public void initializationSucceeded(String clusterName) {
                super.initializationSucceeded(clusterName); // VCs now Serving — race window opens
                initializationCompleted.countDown();
                try {
                    allowStartupToComplete.await();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        this.proxy = new KafkaProxy(configParser, configuration, Features.defaultFeatures(), blockingRegistry);

        var startupResult = new AtomicReference<CompletableFuture<Void>>();
        var startupException = new AtomicReference<Throwable>();
        var startupThread = new Thread(() -> {
            try {
                startupResult.set(proxy.startup());
            }
            catch (Throwable t) {
                startupException.set(t);
            }
        });
        startupThread.start();

        // Wait until we are in the race window: VCs are Serving but startup has not yet
        // attempted the STARTING → STARTED transition.
        assertThat(initializationCompleted.await(5, TimeUnit.SECONDS)).isTrue();

        // Trigger shutdown from this thread: transitions STARTING → STOPPING → STOPPED.
        proxy.shutdown();

        // Release the startup thread to resume past initializationSucceeded().
        allowStartupToComplete.countDown();
        startupThread.join(5_000);

        assertThat(startupException.get()).isNull();
        assertThat(startupResult.get()).isCompletedWithValue(null);
    }

    @Test
    void shouldFailConstructionWhenFilterInitializationFails() {
        // The FCF-per-VC refactor moved filter init into VirtualClusterModel construction (run from
        // KafkaProxy's constructor via defaultRegistry), so a bad filter config now surfaces from the
        // constructor — wrapped as LifecycleException by defaultRegistry — rather than from startup().
        // The proxy object never exists when its construction throws, so the previously-observed
        // post-failure transition to Stopped is no longer reachable; the exception-type contract is the
        // observable contract that remains.
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
        var parsedConfig = configParser.parseConfiguration(config);
        var features = Features.defaultFeatures();

        assertThatThrownBy(() -> new KafkaProxy(configParser, parsedConfig, features))
                .isInstanceOf(LifecycleException.class)
                .cause()
                .isInstanceOf(PluginConfigurationException.class);
    }
}
