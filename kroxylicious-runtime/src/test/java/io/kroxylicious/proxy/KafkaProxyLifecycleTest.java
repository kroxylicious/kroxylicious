/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Serving;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Stopped;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
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
            assertThat(proxy.lifecycleFor("demo1"))
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
            assertThat(proxy.lifecycleFor("cluster-a"))
                    .isNotNull()
                    .satisfies(m -> assertThat(m.state()).isInstanceOf(Serving.class));
            assertThat(proxy.lifecycleFor("cluster-b"))
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
        var manager = proxy.lifecycleFor("demo1");

        // when
        proxy.shutdown();

        // then
        assertThat(manager).isNotNull();
        assertThat(manager.state()).isInstanceOf(Stopped.class);
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

        assertThatThrownBy(() -> new KafkaProxy(configParser, parsedConfig, Features.defaultFeatures()))
                .isInstanceOf(LifecycleException.class)
                .cause()
                .isInstanceOf(PluginConfigurationException.class);
    }
}
