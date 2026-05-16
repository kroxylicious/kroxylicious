/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.PassThroughRouterFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.clients.CloseableAdmin;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests verifying that routing via a pass-through router
 * works identically to the direct target-cluster path.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RoutingPassThroughIT {

    private static final String ROUTE_NAME = "default-route";
    private static final String ROUTER_NAME = "pass-through";
    private static final String TARGET_CLUSTER_NAME = "backing";

    private ConfigurationBuilder routingConfig(KafkaCluster cluster) {
        var targetCluster = new ClusterDefinition(TARGET_CLUSTER_NAME,
                cluster.getBootstrapServers(), null);

        var route = new RouteDefinition(ROUTE_NAME, 0, List.of(), new RouteTarget(TARGET_CLUSTER_NAME, null));

        var routerConfig = new PassThroughRouterFactory.Config(ROUTE_NAME);
        var routerDef = new RouterDefinition(ROUTER_NAME,
                PassThroughRouterFactory.class.getName(), routerConfig, List.of(route));

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
    void shouldProduceAndConsumeViaPassThroughRouter(KafkaCluster cluster, Topic topic) throws Exception {
        var config = routingConfig(cluster);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer();
                var consumer = tester.consumer(
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "routing-test",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            producer.send(new ProducerRecord<>(topic.name(), "key", "value"))
                    .get(10, TimeUnit.SECONDS);

            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records).hasSize(1);
            var record = records.iterator().next();
            assertThat(record.key()).isEqualTo("key");
            assertThat(record.value()).isEqualTo("value");
        }
    }

    @Test
    void shouldListTopicsViaPassThroughRouter(KafkaCluster cluster, Topic topic) throws Exception {
        var config = routingConfig(cluster);

        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin()) {

            var topics = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            assertThat(topics).contains(topic.name());
        }
    }

    @ParameterizedTest(name = "route to {0}")
    @CsvSource({ "route-a", "route-b" })
    void shouldRouteToSelectedClusterWhenMultipleRoutesDefined(String selectedRoute, KafkaCluster clusterA, KafkaCluster clusterB) throws Exception {
        // Given: two clusters, two routes, router statically maps all keys to the selected route
        var clusterDefA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var clusterDefB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);

        var routeA = new RouteDefinition("route-a", 0, List.of(), new RouteTarget("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1, List.of(), new RouteTarget("cluster-b", null));

        var routerConfig = new PassThroughRouterFactory.Config(selectedRoute);
        var routerDef = new RouterDefinition("multi-route-router",
                PassThroughRouterFactory.class.getName(), routerConfig, List.of(routeA, routeB));

        var vc = new VirtualClusterBuilder()
                .withName("multi-route")
                .withTarget(new RouteTarget(null, "multi-route-router"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        var config = baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDefA)
                .addToClusterDefinitions(clusterDefB)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);

        KafkaCluster expectedCluster = selectedRoute.equals("route-a") ? clusterA : clusterB;
        KafkaCluster unexpectedCluster = selectedRoute.equals("route-a") ? clusterB : clusterA;
        var topicName = "route-selection-test-" + UUID.randomUUID();

        // When: produce and consume through the proxy
        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin();
                var producer = tester.producer();
                var consumer = tester.consumer(
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "route-select-test",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);

            producer.send(new ProducerRecord<>(topicName, "key", "routed-value"))
                    .get(10, TimeUnit.SECONDS);

            consumer.subscribe(Set.of(topicName));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records).hasSize(1);
            assertThat(records.iterator().next().value()).isEqualTo("routed-value");
        }

        // Then: data should exist on the selected cluster, not on the other
        try (var expectedAdmin = CloseableAdmin.create(expectedCluster.getKafkaClientConfiguration());
                var unexpectedAdmin = CloseableAdmin.create(unexpectedCluster.getKafkaClientConfiguration())) {
            var expectedTopics = expectedAdmin.listTopics().names().get(10, TimeUnit.SECONDS);
            assertThat(expectedTopics).contains(topicName);

            var unexpectedTopics = unexpectedAdmin.listTopics().names().get(10, TimeUnit.SECONDS);
            assertThat(unexpectedTopics).isEmpty();
        }
    }
}
