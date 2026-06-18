/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.proxy.router.topic.config.RouteConfig;
import io.kroxylicious.proxy.router.topic.config.TopicPartitionRouterConfig;
import io.kroxylicious.proxy.topology.BrokerInfo;
import io.kroxylicious.proxy.topology.Coordinators;
import io.kroxylicious.proxy.topology.PartitionInfo;
import io.kroxylicious.proxy.topology.PartitionLeaders;
import io.kroxylicious.proxy.topology.TopologyService;
import io.kroxylicious.proxy.topology.VirtualNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicPartitionRouterFactoryTest {

    private final TopicPartitionRouterFactory factory = new TopicPartitionRouterFactory();

    @Test
    void shouldInitialiseWithDefaultRouteOnly() {
        var context = testContext(Set.of("fallback"));
        var config = new TopicPartitionRouterConfig("fallback", List.of());

        var initData = factory.initialize(context, config);

        assertThat(initData.routingTable().allRoutes()).containsExactly("fallback");
    }

    @Test
    void shouldInitialiseWithTopicRoutes() {
        var context = testContext(Set.of("cluster-a", "cluster-b", "default-route"));
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
        var context = testContext(Set.of("cluster-a", "cluster-b", "default-route"));
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
        var context = testContext(Set.of("cluster-a", "cluster-b", "default-route"));
        var config = new TopicPartitionRouterConfig("default-route", List.of(
                new RouteConfig("cluster-a", List.of("a."), null, null),
                new RouteConfig("cluster-b", List.of("b."), null, List.of("bob"))));

        var initData = factory.initialize(context, config);

        assertThat(initData.subjectRoutes()).containsEntry("bob", "cluster-b");
    }

    @Test
    void shouldRejectNoRoutesAndNoDefault() {
        var context = testContext(Set.of());
        var config = new TopicPartitionRouterConfig(null, List.of());

        assertThatThrownBy(() -> factory.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("At least one route or a defaultRoute");
    }

    @Test
    void shouldRejectDuplicatePrefixOnDifferentRoutes() {
        var context = testContext(Set.of("a", "b", "default"));
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
        var context = testContext(Set.of("a", "b", "default"));
        var config = new TopicPartitionRouterConfig("default", List.of(
                new RouteConfig("a", List.of("orders.")),
                new RouteConfig("b", List.of("orders.uk."))));

        assertThatThrownBy(() -> factory.initialize(context, config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("disjoint");
    }

    @Test
    void shouldRejectDuplicateExplicitTopicOnDifferentRoutes() {
        var context = testContext(Set.of("a", "b", "default"));
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
        var context = testContext(Set.of("a", "b", "default"));
        var config = new TopicPartitionRouterConfig("default", List.of(
                new RouteConfig("a", List.of("a."), null, List.of("bob")),
                new RouteConfig("b", List.of("b."), null, List.of("bob"))));

        assertThatThrownBy(() -> factory.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("bob")
                .hasMessageContaining("assigned to");
    }

    @Test
    void shouldRejectRouteConfigNameNotInRouteNames() {
        var context = testContext(Set.of("cluster-a", "default"));
        var config = new TopicPartitionRouterConfig("default", List.of(
                new RouteConfig("cluster-a", List.of("a.")),
                new RouteConfig("unknown-route", List.of("b."))));

        assertThatThrownBy(() -> factory.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("unknown-route")
                .hasMessageContaining("not defined");
    }

    @Test
    void shouldRejectDefaultRouteNotInRouteNames() {
        var context = testContext(Set.of("cluster-a"));
        var config = new TopicPartitionRouterConfig("no-such-route", List.of(
                new RouteConfig("cluster-a", List.of("a."))));

        assertThatThrownBy(() -> factory.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("no-such-route")
                .hasMessageContaining("not defined");
    }

    @Test
    void shouldCreateRouter() {
        var context = testContext(Set.of("cluster-a", "default-route"));
        var config = new TopicPartitionRouterConfig("default-route", List.of(
                new RouteConfig("cluster-a", List.of("orders."))));

        var initData = factory.initialize(context, config);
        var router = factory.createRouter(context, initData);

        assertThat(router).isNotNull();
        assertThat(router.staticRoutes()).isNotEmpty();
    }

    private static RouterFactoryContext testContext(Set<String> routeNames) {
        return new RouterFactoryContext() {
            @Override
            public String virtualClusterName() {
                return "testVc";
            }

            @Override
            public String routerName() {
                return "testRouter";
            }

            @Override
            public Set<String> routeNames() {
                return routeNames;
            }

            @Override
            public <P> P pluginInstance(Class<P> pluginClass, String implementationName) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <P> Set<String> pluginImplementationNames(Class<P> pluginClass) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TopologyService topologyService() {
                return new NoOpTopologyService();
            }

            @Override
            public void allowSharedClusterTargets() {
            }
        };
    }

    private static class NoOpTopologyService implements TopologyService {
        @Override
        public CompletionStage<PartitionLeaders> leaders(Map<String, Set<String>> topicsByRoute) {
            return CompletableFuture.completedFuture((topic, partition) -> Optional.empty());
        }

        @Override
        public CompletionStage<Coordinators> coordinators(String route, byte keyType, Set<String> keys) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletionStage<Map<Uuid, String>> topicNames(String route, Set<Uuid> topicIds) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public Optional<PartitionInfo> partitionInfo(String topicName, int partitionIndex) {
            return Optional.empty();
        }

        @Override
        public Optional<BrokerInfo> brokerInfo(VirtualNode node) {
            return Optional.empty();
        }

        @Override
        public void invalidateRoute(String route) {
        }
    }
}
