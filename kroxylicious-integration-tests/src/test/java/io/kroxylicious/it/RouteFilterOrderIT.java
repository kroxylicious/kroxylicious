/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.ClientIdRouterFactory;
import io.kroxylicious.it.testplugins.OrderRecordingFilterFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.RouteDefinition;
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
 * Verifies that per-route filters are applied in the correct order
 * when a routing pipeline contains multiple routing levels
 * (PassthroughRouter + configured Router).
 *
 * <pre>
 * Pipeline:
 *   PassthroughRoutingHandler("default")
 *     → [vc-filter] (route-scoped to "default")
 *     → RoutingDecisionHandler(ClientIdRouter)
 *       → [filter-a] (route-scoped to "route-a") → cluster-A
 *       → [filter-b] (route-scoped to "route-b") → cluster-B
 *     → RoutingTerminalHandler
 * </pre>
 *
 * Expected PRODUCE filter order for route-a:
 *   request:  vc-filter → filter-a
 *   response: filter-a → vc-filter
 *
 * Expected PRODUCE filter order for route-b:
 *   request:  vc-filter → filter-b
 *   response: filter-b → vc-filter
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RouteFilterOrderIT {

    private static final String TOPIC = "route-filter-order-test";

    KafkaCluster clusterA;
    KafkaCluster clusterB;

    @BeforeEach
    void setUp() throws Exception {
        OrderRecordingFilterFactory.reset();
        var newTopic = new NewTopic(TOPIC, Optional.of(1), Optional.empty());
        for (var cluster : List.of(clusterA, clusterB)) {
            try (var admin = AdminClient.create(cluster.getKafkaClientConfiguration())) {
                admin.createTopics(List.of(newTopic)).all().get(10, TimeUnit.SECONDS);
            }
        }
    }

    @AfterEach
    void tearDown() {
        OrderRecordingFilterFactory.reset();
    }

    private ConfigurationBuilder routeFilterConfig() {
        var defA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var defB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);

        var routeA = new RouteDefinition("route-a", 0, List.of("filter-a"),
                new RouteDefinition.Target("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1, List.of("filter-b"),
                new RouteDefinition.Target("cluster-b", null));

        var routerConfig = new ClientIdRouterFactory.Config(
                Map.of("app-a", "route-a", "app-b", "route-b"),
                "route-a");
        var router = new RouterDefinition("test-router",
                ClientIdRouterFactory.class.getName(), routerConfig,
                List.of(routeA, routeB));

        var vcFilter = new NamedFilterDefinitionBuilder(
                "vc-filter",
                OrderRecordingFilterFactory.class.getName())
                .withConfig("name", "vc")
                .build();
        var filterA = new NamedFilterDefinitionBuilder(
                "filter-a",
                OrderRecordingFilterFactory.class.getName())
                .withConfig("name", "route-a")
                .build();
        var filterB = new NamedFilterDefinitionBuilder(
                "filter-b",
                OrderRecordingFilterFactory.class.getName())
                .withConfig("name", "route-b")
                .build();

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteDefinition.Target(null, "test-router"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        return baseConfigurationBuilder()
                .addToClusterDefinitions(defA, defB)
                .addToFilterDefinitions(vcFilter, filterA, filterB)
                .addToDefaultFilters("vc-filter")
                .addToRouterDefinitions(router)
                .addToVirtualClusters(vc);
    }

    @Test
    void routeAFiltersShouldApplyInCorrectOrder() throws Exception {
        try (var tester = kroxyliciousTester(routeFilterConfig());
                var producer = tester.producer(producerConfig("app-a"))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "value-a"))
                    .get(10, TimeUnit.SECONDS);
        }

        var invocations = OrderRecordingFilterFactory.drainInvocations();
        assertThat(invocations)
                .as("VC filter should fire before route-a filter on request, reversed on response")
                .containsExactly(
                        "vc:request",
                        "route-a:request",
                        "route-a:response",
                        "vc:response");
    }

    @Test
    void routeBFiltersShouldApplyInCorrectOrder() throws Exception {
        try (var tester = kroxyliciousTester(routeFilterConfig());
                var producer = tester.producer(producerConfig("app-b"))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "value-b"))
                    .get(10, TimeUnit.SECONDS);
        }

        var invocations = OrderRecordingFilterFactory.drainInvocations();
        assertThat(invocations)
                .as("VC filter should fire before route-b filter on request, reversed on response")
                .containsExactly(
                        "vc:request",
                        "route-b:request",
                        "route-b:response",
                        "vc:response");
    }

    @Test
    void routeFiltersShouldNotCrossContaminate() throws Exception {
        try (var tester = kroxyliciousTester(routeFilterConfig())) {
            try (var producerA = tester.producer(producerConfig("app-a"))) {
                producerA.send(new ProducerRecord<>(TOPIC, "key", "value-a"))
                        .get(10, TimeUnit.SECONDS);
            }
            var afterA = OrderRecordingFilterFactory.drainInvocations();
            assertThat(afterA)
                    .as("route-b filter must not fire for route-a traffic")
                    .noneMatch(s -> s.startsWith("route-b:"));

            try (var producerB = tester.producer(producerConfig("app-b"))) {
                producerB.send(new ProducerRecord<>(TOPIC, "key", "value-b"))
                        .get(10, TimeUnit.SECONDS);
            }
            var afterB = OrderRecordingFilterFactory.drainInvocations();
            assertThat(afterB)
                    .as("route-a filter must not fire for route-b traffic")
                    .noneMatch(s -> s.startsWith("route-a:"));
        }
    }

    private static Map<String, Object> producerConfig(String clientId) {
        var config = new HashMap<String, Object>();
        config.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.RETRIES_CONFIG, 0);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        return config;
    }
}
