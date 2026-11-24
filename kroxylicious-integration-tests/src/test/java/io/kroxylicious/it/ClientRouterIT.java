/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;
import io.micrometer.core.instrument.Metrics;

import io.kroxylicious.it.testplugins.ProduceCountingFilter;
import io.kroxylicious.it.testplugins.ProduceCountingFilterFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.router.client.ClientRouter;
import io.kroxylicious.router.client.Config;
import io.kroxylicious.router.client.Rule;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.client.KafkaClient;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Name;
import io.kroxylicious.testing.kafka.junit5ext.Topic;
import io.kroxylicious.testing.kafka.junit5ext.TopicNameMethodSource;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end tests for the Client Router (design proposal kroxylicious/design#140), proving the
 * router works through the real proxy pipeline: routing a connection to the mapped downstream
 * cluster based on its {@code client.id}, rejecting unmapped connections fail-closed, and applying
 * per-route filters.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class ClientRouterIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();
    private static final String PROXY_ADDRESS = "localhost:0";
    private static final String TOPIC = "client-router-it-topic";

    @Name("clusterA")
    static KafkaCluster clusterA;
    @Name("clusterB")
    static KafkaCluster clusterB;

    @SuppressWarnings("unused") // topic creation managed by KafkaClusterExtension
    @Name("clusterA")
    @TopicNameMethodSource("fixedTopicName")
    static Topic clusterATopic;

    @SuppressWarnings("unused") // topic creation managed by KafkaClusterExtension
    @Name("clusterB")
    @TopicNameMethodSource("fixedTopicName")
    static Topic clusterBTopic;

    @SuppressWarnings("unused") // referenced by @TopicNameMethodSource
    static String fixedTopicName() {
        return TOPIC;
    }

    private List<ConsumerRecord<String, String>> consumeFrom(KafkaCluster cluster, String groupId) {
        Map<String, Object> configs = new HashMap<>(Map.of(
                GROUP_ID_CONFIG, groupId,
                AUTO_OFFSET_RESET_CONFIG, "earliest"));
        try (var consumer = new KafkaConsumer<String, String>(
                consumerProps(cluster, configs))) {
            consumer.subscribe(List.of(TOPIC));
            return StreamSupport.stream(consumer.poll(Duration.ofSeconds(10)).records(TOPIC).spliterator(), false).toList();
        }
    }

    private Map<String, Object> consumerProps(KafkaCluster cluster, Map<String, Object> extra) {
        Map<String, Object> props = new HashMap<>(Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"));
        props.putAll(extra);
        return props;
    }

    @Test
    void unmappedClientIdRejectedFailClosed() {
        // Given: no rule maps the connecting client's client.id to a route
        var clusterDef = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var route = new RouteDefinition("route-a", 0, List.of(), new RouteTarget("cluster-a", null));
        var routerDef = new RouterDefinition("client-router", ClientRouter.class.getName(),
                new Config(List.of()), List.of(route));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "client-router"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS).build())
                .build();
        var config = baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);

        // When / Then
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED)
                .createDefaultKroxyliciousTester()) {
            String[] hostPort = tester.getBootstrapAddress().split(":", -1);
            try (var client = new KafkaClient(hostPort[0], Integer.parseInt(hostPort[1]))) {
                client.getSync(new Request(ApiKeys.METADATA, (short) 9, "unmapped-client", new MetadataRequestData()));
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(client.isOpen()).isFalse());
            }
            assertThat(Metrics.globalRegistry.get("kroxylicious_client_router_rejected_total")
                    .tags("virtual_cluster", "demo", "router", "client-router", "reason", "unmapped")
                    .counter().count()).isEqualTo(1);
        }
    }

    @Test
    void perRouteFilterOnlyAffectsThatRoutesTraffic() {
        // Given: ProduceCountingFilter attached to route-a only
        var counterId = "t5-route-a-counter";
        ProduceCountingFilter.reset(counterId);
        var counter = new NamedFilterDefinitionBuilder(ProduceCountingFilterFactory.class.getName(), ProduceCountingFilterFactory.class.getName())
                .withConfig("counterId", counterId)
                .build();
        var clusterDefA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var clusterDefB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);
        var routeA = new RouteDefinition("route-a", 0, List.of(counter.name()), new RouteTarget("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1, List.of(), new RouteTarget("cluster-b", null));
        var routingRules = List.of(new Rule("alice", "route-a"), new Rule("carol", "route-b"));
        var routerDef = new RouterDefinition("client-router", ClientRouter.class.getName(),
                new Config(routingRules), List.of(routeA, routeB));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "client-router"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS).build())
                .build();
        var config = baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDefA, clusterDefB)
                .addToFilterDefinitions(counter)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);

        // When
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED)
                .createDefaultKroxyliciousTester();
                Producer<String, String> aliceProducer = tester.producer(Map.of(CLIENT_ID_CONFIG, "alice"));
                Producer<String, String> carolProducer = tester.producer(Map.of(CLIENT_ID_CONFIG, "carol"))) {
            assertThat(aliceProducer.send(new ProducerRecord<>(TOPIC, "t5-alice-key", "t5-alice-value")))
                    .succeedsWithin(Duration.ofSeconds(10));
            assertThat(carolProducer.send(new ProducerRecord<>(TOPIC, "t5-carol-key", "t5-carol-value")))
                    .succeedsWithin(Duration.ofSeconds(10));
        }

        // Then: alice's traffic reached cluster-a and passed through route-a's filter;
        // carol's traffic reached cluster-b and bypassed it
        assertThat(consumeFrom(clusterA, "t5-verify-a"))
                .extracting(ConsumerRecord::key)
                .containsExactly("t5-alice-key");
        assertThat(consumeFrom(clusterB, "t5-verify-b"))
                .extracting(ConsumerRecord::key)
                .containsExactly("t5-carol-key");
        assertThat(ProduceCountingFilter.countFor(counterId)).isEqualTo(1);
    }

    @Test
    void hasClientIdRuleRoutesToMappedCluster() {
        // Given: routing decided purely by client.id
        var clusterDefA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var clusterDefB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);
        var routeA = new RouteDefinition("route-a", 0, List.of(), new RouteTarget("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1, List.of(), new RouteTarget("cluster-b", null));
        var routingRules = List.of(new Rule("demo-app", "route-a"));
        var routerDef = new RouterDefinition("client-router", ClientRouter.class.getName(),
                new Config(routingRules), List.of(routeA, routeB));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "client-router"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS).build())
                .build();
        var config = baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDefA, clusterDefB)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);

        // When
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED)
                .createDefaultKroxyliciousTester();
                Producer<String, String> producer = tester.producer(Map.of(CLIENT_ID_CONFIG, "demo-app"))) {
            assertThat(producer.send(new ProducerRecord<>(TOPIC, "t6-demo-key", "t6-demo-value")))
                    .succeedsWithin(Duration.ofSeconds(10));
        }

        // Then: routed to cluster-a, as mapped for client.id "demo-app"
        assertThat(consumeFrom(clusterA, "t6-verify-a"))
                .extracting(ConsumerRecord::key)
                .contains("t6-demo-key");
    }

    @Test
    void twoRoutesCanTargetTheSameCluster() {
        // Given: route-a and route-b both target cluster-a, with a filter attached to route-a only
        var counterId = "t7-route-a-counter";
        ProduceCountingFilter.reset(counterId);
        var counter = new NamedFilterDefinitionBuilder(ProduceCountingFilterFactory.class.getName(), ProduceCountingFilterFactory.class.getName())
                .withConfig("counterId", counterId)
                .build();
        var clusterDefA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var routeA = new RouteDefinition("route-a", 0, List.of(counter.name()), new RouteTarget("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1, List.of(), new RouteTarget("cluster-a", null));
        var routingRules = List.of(new Rule("alice", "route-a"), new Rule("carol", "route-b"));
        var routerDef = new RouterDefinition("client-router", ClientRouter.class.getName(),
                new Config(routingRules), List.of(routeA, routeB));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "client-router"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS).build())
                .build();
        var config = baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDefA)
                .addToFilterDefinitions(counter)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);

        // When
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED)
                .createDefaultKroxyliciousTester();
                Producer<String, String> aliceProducer = tester.producer(Map.of(CLIENT_ID_CONFIG, "alice"));
                Producer<String, String> carolProducer = tester.producer(Map.of(CLIENT_ID_CONFIG, "carol"))) {
            assertThat(aliceProducer.send(new ProducerRecord<>(TOPIC, "t7-alice-key", "t7-alice-value")))
                    .succeedsWithin(Duration.ofSeconds(10));
            assertThat(carolProducer.send(new ProducerRecord<>(TOPIC, "t7-carol-key", "t7-carol-value")))
                    .succeedsWithin(Duration.ofSeconds(10));
        }

        // Then: both clients' traffic reached cluster-a, but only route-a's (alice's) traffic
        // passed through the filter attached to route-a
        assertThat(consumeFrom(clusterA, "t7-verify-a"))
                .extracting(ConsumerRecord::key)
                .contains("t7-alice-key", "t7-carol-key");
        assertThat(ProduceCountingFilter.countFor(counterId)).isEqualTo(1);
    }
}
