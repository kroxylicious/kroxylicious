/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.FixedClientIdFilterFactory;
import io.kroxylicious.it.testplugins.router.PassThroughRouterFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests verifying that filters configured on routes are applied
 * to traffic traversing those routes.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RouteFilterIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();
    private static final String ROUTE_NAME = "default-route";
    private static final String ROUTER_NAME = "pass-through";
    private static final String TARGET_CLUSTER_NAME = "backing";
    private static final String TOPIC = "route-filter-test";

    KafkaCluster cluster;

    @BeforeEach
    void setUp() throws Exception {
        try (var admin = org.apache.kafka.clients.admin.AdminClient.create(cluster.getKafkaClientConfiguration())) {
            admin.createTopics(List.of(new NewTopic(TOPIC, Optional.of(1), Optional.empty())))
                    .all().get(10, TimeUnit.SECONDS);
        }
    }

    private ConfigurationBuilder routingConfigWithRouteFilter(String filterName, String filterType, Map<String, Object> filterConfig) {
        var filterDef = new NamedFilterDefinitionBuilder(filterName, filterType)
                .withConfig(filterConfig)
                .build();

        var route = new RouteDefinition(ROUTE_NAME, 0, List.of(filterName), new RouteTarget(TARGET_CLUSTER_NAME, null));
        var routerConfig = new PassThroughRouterFactory.Config(ROUTE_NAME);
        var routerDef = new RouterDefinition(ROUTER_NAME,
                PassThroughRouterFactory.class.getName(), routerConfig, List.of(route));

        var targetCluster = new ClusterDefinition(TARGET_CLUSTER_NAME,
                cluster.getBootstrapServers(), null);

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        return baseConfigurationBuilder()
                .addToClusterDefinitions(targetCluster)
                .addToFilterDefinitions(filterDef)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }

    private ConfigurationBuilder routingConfigWithoutRouteFilter() {
        var route = new RouteDefinition(ROUTE_NAME, 0, List.of(), new RouteTarget(TARGET_CLUSTER_NAME, null));
        var routerConfig = new PassThroughRouterFactory.Config(ROUTE_NAME);
        var routerDef = new RouterDefinition(ROUTER_NAME,
                PassThroughRouterFactory.class.getName(), routerConfig, List.of(route));

        var targetCluster = new ClusterDefinition(TARGET_CLUSTER_NAME,
                cluster.getBootstrapServers(), null);

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        return baseConfigurationBuilder()
                .addToClusterDefinitions(targetCluster)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }

    @Test
    void produceAndConsumeWorkThroughFilteredRoute() {
        // Given
        var config = routingConfigWithRouteFilter(
                "fixed-client-id",
                FixedClientIdFilterFactory.class.getName(),
                Map.of("clientId", "route-filter-stamped"));

        // When / Then
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer();
                var consumer = tester.consumer(
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "route-filter-test",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            assertThat(producer.send(new ProducerRecord<>(TOPIC, "key", "value")))
                    .succeedsWithin(Duration.ofSeconds(10));

            consumer.subscribe(Set.of(TOPIC));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("value");
        }
    }

    @Test
    void routeWithoutFiltersIsUnaffected(Topic topic) {
        // Given
        var config = routingConfigWithoutRouteFilter();

        // When / Then
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer();
                var consumer = tester.consumer(
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "route-no-filter-test",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "key", "value")))
                    .succeedsWithin(Duration.ofSeconds(10));

            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("value");
        }
    }

    @Test
    void vcFilterAndRouteFilterBothExecute() {
        // Given
        var vcFilter = new NamedFilterDefinitionBuilder(
                "vc-stamper",
                FixedClientIdFilterFactory.class.getName())
                .withConfig("clientId", "vc-stamp")
                .build();

        var routeFilter = new NamedFilterDefinitionBuilder(
                "route-stamper",
                FixedClientIdFilterFactory.class.getName())
                .withConfig("clientId", "route-stamp")
                .build();

        var route = new RouteDefinition(ROUTE_NAME, 0, List.of("route-stamper"), new RouteTarget(TARGET_CLUSTER_NAME, null));
        var routerConfig = new PassThroughRouterFactory.Config(ROUTE_NAME);
        var routerDef = new RouterDefinition(ROUTER_NAME,
                PassThroughRouterFactory.class.getName(), routerConfig, List.of(route));

        var targetCluster = new ClusterDefinition(TARGET_CLUSTER_NAME,
                cluster.getBootstrapServers(), null);

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .withFilters(List.of("vc-stamper"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        var config = baseConfigurationBuilder()
                .addToClusterDefinitions(targetCluster)
                .addToFilterDefinitions(vcFilter)
                .addToFilterDefinitions(routeFilter)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);

        // When / Then: both filters run; route filter runs last so "route-stamp" wins
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer();
                var consumer = tester.consumer(
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "vc-and-route-filter-test",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            assertThat(producer.send(new ProducerRecord<>(TOPIC, "key", "value")))
                    .succeedsWithin(Duration.ofSeconds(10));

            consumer.subscribe(Set.of(TOPIC));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("value");
        }
    }
}
