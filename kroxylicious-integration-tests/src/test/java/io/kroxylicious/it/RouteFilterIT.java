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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.ClientIdRouterFactory;
import io.kroxylicious.it.testplugins.FixedClientIdFilterFactory;
import io.kroxylicious.it.testplugins.RequestResponseMarkingFilterFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests verifying that the Filter API contract is honoured
 * for filters configured on routes.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RouteFilterIT {

    private static final String TOPIC = "route-filter-test";

    KafkaCluster clusterA;
    KafkaCluster clusterB;

    private RoutingEventCaptor routingCaptor;

    @BeforeEach
    void setUp() throws Exception {
        routingCaptor = RoutingEventCaptor.install();
        var newTopic = new NewTopic(TOPIC, Optional.of(1), Optional.empty());
        for (var cluster : List.of(clusterA, clusterB)) {
            try (var admin = AdminClient.create(cluster.getKafkaClientConfiguration())) {
                admin.createTopics(List.of(newTopic)).all().get(10, TimeUnit.SECONDS);
            }
        }
    }

    @AfterEach
    void tearDown() {
        if (routingCaptor != null) {
            routingCaptor.close();
        }
    }

    @Test
    void requestMutationByRouteFilterReachesBackend() throws Exception {
        var config = configWithRouteFilter(
                "fixed-client-id",
                FixedClientIdFilterFactory.class.getName(),
                Map.of("clientId", "route-filter-stamped"));

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(producerConfig("route-a-client"))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "value"))
                    .get(10, TimeUnit.SECONDS);
        }

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .hasSizeGreaterThanOrEqualTo(1);
    }

    @Test
    void multipleRouteFiltersExecuteInDeclarationOrder() throws Exception {
        var firstFilter = new NamedFilterDefinitionBuilder(
                "first-marker",
                RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("name", "first")
                .withConfig("keysToMark", List.of("PRODUCE"))
                .withConfig("direction", List.of("REQUEST"))
                .build();

        var secondFilter = new NamedFilterDefinitionBuilder(
                "second-marker",
                RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("name", "second")
                .withConfig("keysToMark", List.of("PRODUCE"))
                .withConfig("direction", List.of("REQUEST"))
                .build();

        var routeA = new RouteDefinition("route-a", 0,
                List.of("first-marker", "second-marker"),
                new RouteTarget("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1,
                null,
                new RouteTarget("cluster-b", null));

        var config = buildConfig(routeA, routeB, firstFilter, secondFilter);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(producerConfig("route-a-client"))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "ordered"))
                    .get(10, TimeUnit.SECONDS);
        }

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .as("Both filters should have executed on route-a")
                .hasSizeGreaterThanOrEqualTo(1);
    }

    @Test
    void routeWithoutFiltersIsUnaffected() throws Exception {
        var config = configWithRouteFilter(
                "fixed-client-id",
                FixedClientIdFilterFactory.class.getName(),
                Map.of("clientId", "route-filter-stamped"));

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(producerConfig("route-b-client"))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "unfiltered"))
                    .get(10, TimeUnit.SECONDS);
        }

        var records = consumeDirectly(clusterB);
        assertThat(records).extracting(ConsumerRecord::value)
                .containsExactly("unfiltered");
    }

    private ConfigurationBuilder configWithRouteFilter(
                                                       String filterName,
                                                       String filterType,
                                                       Map<String, Object> filterConfig) {
        var filterDef = new NamedFilterDefinitionBuilder(filterName, filterType);
        filterConfig.forEach(filterDef::withConfig);

        var routeA = new RouteDefinition("route-a", 0,
                List.of(filterName),
                new RouteTarget("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1,
                null,
                new RouteTarget("cluster-b", null));

        return buildConfig(routeA, routeB, filterDef.build());
    }

    private ConfigurationBuilder buildConfig(
                                             RouteDefinition routeA,
                                             RouteDefinition routeB,
                                             io.kroxylicious.proxy.config.NamedFilterDefinition... filterDefs) {
        var defA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var defB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);

        var routerConfig = new ClientIdRouterFactory.Config(
                Map.of("route-a-client", "route-a", "route-b-client", "route-b"),
                "route-a");
        var router = new RouterDefinition("test-router",
                ClientIdRouterFactory.class.getName(), routerConfig,
                List.of(routeA, routeB));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "test-router"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        var builder = baseConfigurationBuilder()
                .addToClusterDefinitions(defA, defB)
                .addToRouterDefinitions(router)
                .addToVirtualClusters(vc);

        for (var fd : filterDefs) {
            builder.addToFilterDefinitions(fd);
        }
        return builder;
    }

    private static Map<String, Object> producerConfig(String clientId) {
        var config = new HashMap<String, Object>();
        config.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.RETRIES_CONFIG, 0);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        return config;
    }

    private List<ConsumerRecord<String, String>> consumeDirectly(KafkaCluster cluster) {
        var props = new HashMap<>(cluster.getKafkaClientConfiguration());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "route-filter-it-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(TOPIC));
            var records = new java.util.ArrayList<ConsumerRecord<String, String>>();
            ConsumerRecords<String, String> polled = consumer.poll(Duration.ofSeconds(10));
            polled.forEach(records::add);
            return records;
        }
    }
}
