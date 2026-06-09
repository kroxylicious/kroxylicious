/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.router.topic;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.router.topic.config.RouteConfig;
import io.kroxylicious.router.topic.config.TopicPartitionRouterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicPartitionRouterFactoryTest {

    private final TopicPartitionRouterFactory factory = new TopicPartitionRouterFactory();
    private final RouterFactoryContext context = testContext("testVc", "testRouter");

    @Test
    void shouldInitialiseWithDefaultRouteOnly() {
        var config = new TopicPartitionRouterConfig("fallback", List.of());

        var initData = factory.initialize(context, config);

        assertThat(initData.routingTable().allRoutes()).containsExactly("fallback");
    }

    @Test
    void shouldInitialiseWithTopicRoutes() {
        var config = new TopicPartitionRouterConfig("default-route", List.of(
                new RouteConfig("cluster-a", List.of("orders.", "payments.")),
                new RouteConfig("cluster-b", List.of("logs."))));

        var initData = factory.initialize(context, config);

        assertThat(initData.routingTable().allRoutes())
                .containsExactlyInAnyOrder("cluster-a", "cluster-b", "default-route");
        assertThat(initData.routingTable().routeForTopic("orders.uk")).isEqualTo("cluster-a");
        assertThat(initData.routingTable().routeForTopic("logs.app")).isEqualTo("cluster-b");
        assertThat(initData.routingTable().routeForTopic("unknown")).isEqualTo("default-route");
    }

    @Test
    void shouldInitialiseWithExplicitTopics() {
        var config = new TopicPartitionRouterConfig("default-route", List.of(
                new RouteConfig("cluster-a", List.of("orders."), null, null),
                new RouteConfig("cluster-b", null, List.of("special-topic"), null)));

        var initData = factory.initialize(context, config);

        assertThat(initData.routingTable().allRoutes())
                .containsExactlyInAnyOrder("cluster-a", "cluster-b", "default-route");
        assertThat(initData.routingTable().routeForTopic("orders.uk")).isEqualTo("cluster-a");
        assertThat(initData.routingTable().routeForTopic("special-topic")).isEqualTo("cluster-b");
        assertThat(initData.routingTable().routeForTopic("unknown")).isEqualTo("default-route");
    }

    @Test
    void shouldInitialiseWithSubjects() {
        var config = new TopicPartitionRouterConfig("default-route", List.of(
                new RouteConfig("cluster-a", List.of("a."), null, null),
                new RouteConfig("cluster-b", List.of("b."), null, List.of("bob"))));

        var initData = factory.initialize(context, config);

        assertThat(initData.subjectRoutes()).containsEntry("bob", "cluster-b");
    }

    @Test
    void shouldRejectNoRoutesAndNoDefault() {
        var config = new TopicPartitionRouterConfig(null, List.of());

        assertThatThrownBy(() -> factory.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("At least one route or a defaultRoute");
    }

    @Test
    void shouldRejectDuplicatePrefixOnDifferentRoutes() {
        var config = new TopicPartitionRouterConfig("default", List.of(
                new RouteConfig("a", List.of("orders.")),
                new RouteConfig("b", List.of("orders."))));

        assertThatThrownBy(() -> factory.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("orders.")
                .hasMessageContaining("assigned to both");
    }

    @Test
    void shouldRejectOverlappingPrefixesOnDifferentRoutes() {
        var config = new TopicPartitionRouterConfig("default", List.of(
                new RouteConfig("a", List.of("orders.")),
                new RouteConfig("b", List.of("orders.uk."))));

        assertThatThrownBy(() -> factory.initialize(context, config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("disjoint");
    }

    @Test
    void shouldRejectDuplicateExplicitTopicOnDifferentRoutes() {
        var config = new TopicPartitionRouterConfig("default", List.of(
                new RouteConfig("a", null, List.of("my-topic"), null),
                new RouteConfig("b", null, List.of("my-topic"), null)));

        assertThatThrownBy(() -> factory.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("my-topic")
                .hasMessageContaining("assigned to both");
    }

    @Test
    void shouldRejectDuplicateSubjectOnDifferentRoutes() {
        var config = new TopicPartitionRouterConfig("default", List.of(
                new RouteConfig("a", List.of("a."), null, List.of("bob")),
                new RouteConfig("b", List.of("b."), null, List.of("bob"))));

        assertThatThrownBy(() -> factory.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("bob")
                .hasMessageContaining("assigned to");
    }

    @Test
    void shouldCreateRouter() {
        var config = new TopicPartitionRouterConfig("default-route", List.of(
                new RouteConfig("cluster-a", List.of("orders."))));

        var initData = factory.initialize(context, config);
        var router = factory.createRouter(context, initData);

        assertThat(router).isNotNull();
        assertThat(router.staticRoutes()).isNotEmpty();
    }

    private static RouterFactoryContext testContext(String vcName, String routerName) {
        return new RouterFactoryContext() {
            @Override
            public String virtualClusterName() {
                return vcName;
            }

            @Override
            public String routerName() {
                return routerName;
            }

            @Override
            public <P> P pluginInstance(Class<P> pluginClass, String implementationName) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <P> Set<String> pluginImplementationNames(Class<P> pluginClass) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
