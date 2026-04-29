/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Serving;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Stopped;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaProxyLifecycleTest {

    private ConfigParser configParser;

    @BeforeEach
    void setUp() {
        configParser = new ConfigParser();
    }

    @Test
    void shouldTrackVirtualClusterAsServingAfterStartup() throws Exception {
        // given
        var config = """
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
            // when
            proxy.startup();

            // then
            assertThat(proxy.lifecycleManagerFor("demo1"))
                    .isNotNull()
                    .satisfies(m -> assertThat(m.state()).isInstanceOf(Serving.class));
        }
    }

    @Test
    void shouldTrackMultipleVirtualClustersAsServing() throws Exception {
        // given
        var config = """
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

        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            // when
            proxy.startup();

            // then
            assertThat(proxy.lifecycleManagerFor("cluster-a"))
                    .isNotNull()
                    .satisfies(m -> assertThat(m.state()).isInstanceOf(Serving.class));
            assertThat(proxy.lifecycleManagerFor("cluster-b"))
                    .isNotNull()
                    .satisfies(m -> assertThat(m.state()).isInstanceOf(Serving.class));
        }
    }

    @Test
    void shouldTransitionToStoppedAfterShutdown() {
        // given
        var config = """
                   virtualClusters:
                     - name: demo1
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: localhost:9192
                """;

        var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures());
        proxy.startup();
        var manager = proxy.lifecycleManagerFor("demo1");

        // when
        proxy.shutdown();

        // then
        assertThat(manager).isNotNull();
        assertThat(manager.state()).isInstanceOf(Stopped.class);
    }

    @Test
    void startupReturnsFutureThatCompletesOnShutdown() {
        var config = """
                   virtualClusters:
                     - name: demo1
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: localhost:9192
                """;

        var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures());
        CompletableFuture<Void> future = proxy.startup();
        assertThat(future).isNotNull().isNotDone();
        proxy.shutdown();
        assertThat(future).isCompletedWithValue(null);
    }

    @Test
    void startupTwiceReturnsSameFutureInstance() {
        var config = """
                   virtualClusters:
                     - name: demo1
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: localhost:9192
                """;

        var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures());
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
        var config = """
                   virtualClusters:
                     - name: demo1
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: localhost:9192
                """;

        var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures());
        assertThatCode(proxy::shutdown).doesNotThrowAnyException();
    }

    @Test
    void shutdownTwiceIsNoOp() {
        var config = """
                   virtualClusters:
                     - name: demo1
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: localhost:9192
                """;

        var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures());
        proxy.startup();
        proxy.shutdown();
        assertThatCode(proxy::shutdown).doesNotThrowAnyException();
    }

    @Test
    void startupAfterStopThrowsIllegalState() {
        var config = """
                   virtualClusters:
                     - name: demo1
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: localhost:9192
                """;

        var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures());
        proxy.startup();
        proxy.shutdown();
        assertThatThrownBy(proxy::startup)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("KafkaProxy is not restartable");
    }

    @Test
    void shouldTransitionToStoppedOnStartupFailure() throws Exception {
        // given
        var config = """
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

        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            // when
            assertThatThrownBy(proxy::startup)
                    .isInstanceOf(LifecycleException.class)
                    .cause()
                    .isInstanceOf(PluginConfigurationException.class);

            // then
            var manager = proxy.lifecycleManagerFor("demo1");
            assertThat(manager).isNotNull();
            assertThat(manager.state())
                    .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.type(Stopped.class))
                    .extracting(Stopped::priorFailureCause)
                    .isInstanceOf(PluginConfigurationException.class);
        }
    }
}
