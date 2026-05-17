/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.routing.topic.config.TopicPartitionRouterConfig;
import io.kroxylicious.proxy.routing.topic.config.TopicRoute;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicPartitionRouterFactoryTest {

    private final TopicPartitionRouterFactory factory = new TopicPartitionRouterFactory();

    @Test
    void shouldInitialiseWithDefaultRouteOnly() {
        var config = new TopicPartitionRouterConfig("fallback", List.of());

        var initData = factory.initialize(null, config);

        assertThat(initData.routingTable().allRoutes()).containsExactly("fallback");
    }

    @Test
    void shouldInitialiseWithTopicRoutes() {
        var config = new TopicPartitionRouterConfig("default-route", List.of(
                new TopicRoute("cluster-a", List.of("orders.", "payments.")),
                new TopicRoute("cluster-b", List.of("logs."))));

        var initData = factory.initialize(null, config);

        assertThat(initData.routingTable().allRoutes())
                .containsExactlyInAnyOrder("cluster-a", "cluster-b", "default-route");
        assertThat(initData.routingTable().routeForTopic("orders.uk")).isEqualTo("cluster-a");
        assertThat(initData.routingTable().routeForTopic("logs.app")).isEqualTo("cluster-b");
        assertThat(initData.routingTable().routeForTopic("unknown")).isEqualTo("default-route");
    }

    @Test
    void shouldRejectNoRoutesAndNoDefault() {
        var config = new TopicPartitionRouterConfig(null, List.of());

        assertThatThrownBy(() -> factory.initialize(null, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("At least one topicRoute or a defaultRoute");
    }

    @Test
    void shouldRejectDuplicatePrefixOnDifferentRoutes() {
        var config = new TopicPartitionRouterConfig("default", List.of(
                new TopicRoute("a", List.of("orders.")),
                new TopicRoute("b", List.of("orders."))));

        assertThatThrownBy(() -> factory.initialize(null, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("orders.")
                .hasMessageContaining("assigned to both");
    }

    @Test
    void shouldRejectOverlappingPrefixesOnDifferentRoutes() {
        var config = new TopicPartitionRouterConfig("default", List.of(
                new TopicRoute("a", List.of("orders.")),
                new TopicRoute("b", List.of("orders.uk."))));

        assertThatThrownBy(() -> factory.initialize(null, config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("disjoint");
    }

    @Test
    void shouldCreateRouter() {
        var config = new TopicPartitionRouterConfig("default-route", List.of(
                new TopicRoute("cluster-a", List.of("orders."))));

        var initData = factory.initialize(null, config);
        var router = factory.createRouter(null, initData);

        assertThat(router).isNotNull();
        assertThat(router.staticRoutes()).isNotEmpty();
    }
}
