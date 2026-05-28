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

import io.kroxylicious.it.testplugins.TagCapturingFilterFactory;
import io.kroxylicious.it.testplugins.TaggingRouterFactory;
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
 * Proves that route filters are applied between routers in a nested
 * routing chain, and that each filter observes the accumulated
 * modifications from all routers that preceded it.
 *
 * <pre>
 * Pipeline:
 *   PassthroughRoutingHandler("default")
 *     → RoutingDecisionHandler(outer-router, activates on "default")
 *         outer-router tags body with "outer"
 *         → sends to "route-nested" → inner-router
 *     → [filter-outer] (route-scoped to "route-nested")
 *         sees tags: [outer]
 *     → RoutingDecisionHandler(inner-router, activates on "route-nested")
 *         inner-router tags body with "inner"
 *         → sends to "route-leaf" → cluster
 *     → [filter-inner] (route-scoped to "route-leaf")
 *         sees tags: [outer, inner]
 *     → RoutingTerminalHandler → cluster
 * </pre>
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class NestedRouteFilterOrderIT {

    private static final String TOPIC = "nested-route-filter-order-test";

    KafkaCluster cluster;

    @BeforeEach
    void setUp() throws Exception {
        TagCapturingFilterFactory.reset();
        try (var admin = AdminClient.create(cluster.getKafkaClientConfiguration())) {
            admin.createTopics(List.of(new NewTopic(TOPIC, Optional.of(1), Optional.empty())))
                    .all().get(10, TimeUnit.SECONDS);
        }
    }

    @AfterEach
    void tearDown() {
        TagCapturingFilterFactory.reset();
    }

    private ConfigurationBuilder nestedTaggingConfig() {
        var clusterDef = new ClusterDefinition("backend", cluster.getBootstrapServers(), null);

        // Inner router: tags body with "inner", forwards to route-leaf → cluster
        var innerRoute = new RouteDefinition("route-leaf", 0, List.of("filter-inner"),
                new RouteDefinition.Target("backend", null));
        var innerRouter = new RouterDefinition("inner-router",
                TaggingRouterFactory.class.getName(),
                new TaggingRouterFactory.Config("inner", 1, "route-leaf"),
                List.of(innerRoute));

        // Outer router: tags body with "outer", forwards to route-nested → inner-router
        var outerRoute = new RouteDefinition("route-nested", 0, List.of("filter-outer"),
                new RouteDefinition.Target(null, "inner-router"));
        var outerRouter = new RouterDefinition("outer-router",
                TaggingRouterFactory.class.getName(),
                new TaggingRouterFactory.Config("outer", 0, "route-nested"),
                List.of(outerRoute));

        var filterOuter = new NamedFilterDefinitionBuilder(
                "filter-outer", TagCapturingFilterFactory.class.getName())
                .withConfig("name", "filter-outer")
                .build();
        var filterInner = new NamedFilterDefinitionBuilder(
                "filter-inner", TagCapturingFilterFactory.class.getName())
                .withConfig("name", "filter-inner")
                .build();

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteDefinition.Target(null, "outer-router"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        return baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToFilterDefinitions(filterOuter, filterInner)
                .addToRouterDefinitions(outerRouter, innerRouter)
                .addToVirtualClusters(vc);
    }

    @Test
    void routeFiltersShouldSeeAccumulatedRouterModifications() throws Exception {
        try (var tester = kroxyliciousTester(nestedTaggingConfig());
                var producer = tester.producer(producerConfig())) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "value"))
                    .get(10, TimeUnit.SECONDS);
        }

        var captures = TagCapturingFilterFactory.drainCaptures();

        // filter-outer is between outer-router and inner-router.
        // It should see only the outer router's tag.
        assertThat(captures)
                .filteredOn(s -> s.startsWith("filter-outer:request:"))
                .first()
                .isEqualTo("filter-outer:request:[outer]");

        // filter-inner is after inner-router.
        // It should see both the outer and inner router tags.
        assertThat(captures)
                .filteredOn(s -> s.startsWith("filter-inner:request:"))
                .first()
                .isEqualTo("filter-inner:request:[outer, inner]");

        // On the response path the order reverses:
        // filter-inner fires before filter-outer
        var requestCaptures = captures.stream().filter(s -> s.contains(":request:")).toList();
        var responseCaptures = captures.stream().filter(s -> s.contains(":response:")).toList();
        assertThat(requestCaptures).extracting(s -> s.split(":")[0])
                .as("Request order: filter-outer before filter-inner")
                .containsExactly("filter-outer", "filter-inner");
        assertThat(responseCaptures).extracting(s -> s.split(":")[0])
                .as("Response order: filter-inner before filter-outer")
                .containsExactly("filter-inner", "filter-outer");
    }

    private static Map<String, Object> producerConfig() {
        var config = new HashMap<String, Object>();
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.RETRIES_CONFIG, 0);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        return config;
    }
}
