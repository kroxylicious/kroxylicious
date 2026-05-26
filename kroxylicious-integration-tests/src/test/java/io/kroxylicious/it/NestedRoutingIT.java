/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.ClientIdRouterFactory;
import io.kroxylicious.it.testplugins.PrincipalRouterFactory;
import io.kroxylicious.it.testplugins.SaslPlainTermination;
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
 * Integration tests for nested router dispatch: an outer router
 * (by principal) targeting an inner router (by client ID),
 * verifying that data reaches the correct backing cluster.
 *
 * <pre>
 * client → VC (SaslPlainTermination: alice/bob)
 *     → outer-router (PrincipalRouter)
 *         alice → route-nested → inner-router (ClientIdRouter)
 *                                    app-1 → route-a → cluster-A
 *                                    app-2 → route-b → cluster-B
 *         bob → route-direct → cluster-C
 * </pre>
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class NestedRoutingIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(NestedRoutingIT.class);
    private static final String TOPIC = "nested-routing-test";

    KafkaCluster clusterA;
    KafkaCluster clusterB;
    KafkaCluster clusterC;

    private RoutingEventCaptor routingCaptor;

    @BeforeEach
    void setUp() throws Exception {
        routingCaptor = RoutingEventCaptor.install();
        var newTopic = new NewTopic(TOPIC, Optional.of(1), Optional.empty());
        for (var cluster : List.of(clusterA, clusterB, clusterC)) {
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

    private ConfigurationBuilder nestedRoutingConfig() {
        var defA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var defB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);
        var defC = new ClusterDefinition("cluster-c", clusterC.getBootstrapServers(), null);

        // Inner router: routes by client ID to cluster-A or cluster-B
        var innerRouteA = new RouteDefinition("route-a", 0, null,
                new RouteTarget("cluster-a", null));
        var innerRouteB = new RouteDefinition("route-b", 1, null,
                new RouteTarget("cluster-b", null));
        var innerConfig = new ClientIdRouterFactory.Config(
                Map.of("app-1", "route-a", "app-2", "route-b"),
                "route-a");
        var innerRouter = new RouterDefinition("inner-router",
                ClientIdRouterFactory.class.getName(), innerConfig,
                List.of(innerRouteA, innerRouteB));

        // Outer router: routes by principal to inner-router or cluster-C
        var outerRouteNested = new RouteDefinition("route-nested", 0, null,
                new RouteTarget(null, "inner-router"));
        var outerRouteDirect = new RouteDefinition("route-direct", 1, null,
                new RouteTarget("cluster-c", null));
        var outerConfig = new PrincipalRouterFactory.Config(
                Map.of("alice", "route-nested", "bob", "route-direct"),
                "route-direct");
        var outerRouter = new RouterDefinition("outer-router",
                PrincipalRouterFactory.class.getName(), outerConfig,
                List.of(outerRouteNested, outerRouteDirect));

        // SASL filter
        var saslFilter = new NamedFilterDefinitionBuilder(
                "sasl-plain",
                SaslPlainTermination.class.getName())
                .withConfig("userNameToPassword",
                        Map.of("alice", "alice-secret", "bob", "bob-secret"))
                .build();

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "outer-router"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        return baseConfigurationBuilder()
                .addToClusterDefinitions(defA, defB, defC)
                .addToFilterDefinitions(saslFilter)
                .addToDefaultFilters("sasl-plain")
                .addToRouterDefinitions(outerRouter, innerRouter)
                .addToVirtualClusters(vc);
    }

    @Test
    void shouldRouteAliceThroughNestedRouterToClusterA() throws Exception {
        try (var tester = kroxyliciousTester(nestedRoutingConfig());
                var producer = tester.producer(producerConfig("alice", "alice-secret", "app-1"))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "alice-app1"))
                    .get(10, TimeUnit.SECONDS);
        }

        assertThat(consumeDirectly(clusterA)).extracting(ConsumerRecord::value)
                .containsExactly("alice-app1");
        assertThat(consumeDirectly(clusterB)).isEmpty();
        assertThat(consumeDirectly(clusterC)).isEmpty();
    }

    @Test
    void shouldRouteAliceToSecondClusterByClientId() throws Exception {
        try (var tester = kroxyliciousTester(nestedRoutingConfig());
                var producer = tester.producer(producerConfig("alice", "alice-secret", "app-2"))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "alice-app2"))
                    .get(10, TimeUnit.SECONDS);
        }

        assertThat(consumeDirectly(clusterA)).isEmpty();
        assertThat(consumeDirectly(clusterB)).extracting(ConsumerRecord::value)
                .containsExactly("alice-app2");
        assertThat(consumeDirectly(clusterC)).isEmpty();
    }

    @Test
    void shouldRouteBobDirectlyToClusterC() throws Exception {
        try (var tester = kroxyliciousTester(nestedRoutingConfig());
                var producer = tester.producer(producerConfig("bob", "bob-secret", "any-client"))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "bob-data"))
                    .get(10, TimeUnit.SECONDS);
        }

        assertThat(consumeDirectly(clusterA)).isEmpty();
        assertThat(consumeDirectly(clusterB)).isEmpty();
        assertThat(consumeDirectly(clusterC)).extracting(ConsumerRecord::value)
                .containsExactly("bob-data");
    }

    @Test
    void shouldEmitRoutingEventsForNestedPath() throws Exception {
        try (var tester = kroxyliciousTester(nestedRoutingConfig());
                var producer = tester.producer(producerConfig("alice", "alice-secret", "app-1"))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "v1"))
                    .get(10, TimeUnit.SECONDS);
        }

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .as("PRODUCE should reach inner router's route-a")
                .hasSizeGreaterThanOrEqualTo(1);
    }

    private static Map<String, Object> producerConfig(String username,
                                                      String password,
                                                      String clientId) {
        var config = new HashMap<String, Object>();
        config.put("security.protocol", "SASL_PLAINTEXT");
        config.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        config.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required"
                        + " username=\"" + username + "\""
                        + " password=\"" + password + "\";");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.RETRIES_CONFIG, 0);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        return config;
    }

    private List<ConsumerRecord<String, String>> consumeDirectly(KafkaCluster cluster) {
        var props = new HashMap<>(cluster.getKafkaClientConfiguration());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(Set.of(TOPIC));
            List<ConsumerRecord<String, String>> all = new ArrayList<>();
            int consecutiveEmpty = 0;
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline && consecutiveEmpty < 3) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
                batch.forEach(all::add);
                if (batch.isEmpty() && !all.isEmpty()) {
                    consecutiveEmpty++;
                }
                else if (!batch.isEmpty()) {
                    consecutiveEmpty = 0;
                }
            }
            return all;
        }
    }
}
