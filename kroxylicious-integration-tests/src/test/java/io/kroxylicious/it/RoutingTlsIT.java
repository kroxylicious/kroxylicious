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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
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
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.clients.CloseableAdmin;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.common.Tls;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Name;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests verifying that upstream TLS is applied per-route, not at the virtual-cluster level.
 * Covers the bug described in issue #4240.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RoutingTlsIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();
    private static final String ROUTER_NAME = "tls-test-router";
    private static final String TLS_ROUTE = "tls-route";
    private static final String PLAIN_ROUTE = "plain-route";
    private static final String TLS_CLUSTER_DEF = "tls-cluster";
    private static final String PLAIN_CLUSTER_DEF = "plain-cluster";

    private io.kroxylicious.proxy.config.tls.Tls upstreamTlsConfig(KafkaCluster tlsCluster) {
        var truststore = (String) tlsCluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        var password = (String) tlsCluster.getKafkaClientConfiguration().get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        return new io.kroxylicious.proxy.config.tls.Tls(null, new TrustStore(truststore, new InlinePassword(password), null), null, null);
    }

    @Test
    void singleRouteTls(@Tls @BrokerCluster(numBrokers = 1) KafkaCluster tlsCluster, Topic topic) {
        // Given: one route targeting a TLS cluster
        var clusterDef = new ClusterDefinition(TLS_CLUSTER_DEF, tlsCluster.getBootstrapServers(), upstreamTlsConfig(tlsCluster));
        var route = new RouteDefinition(TLS_ROUTE, 0, List.of(), new RouteTarget(TLS_CLUSTER_DEF, null));
        var config = buildConfig(clusterDef, List.of(route), TLS_ROUTE);

        // When: produce and consume through the proxy
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer();
                var consumer = tester.consumer(
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "routing-tls-single",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "k", "tls-value")))
                    .succeedsWithin(Duration.ofSeconds(10));

            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));

            // Then: proxy successfully connected via TLS upstream
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("tls-value");
        }
    }

    @Test
    void tlsRouteUsedInMixedConfig(
                                   @Tls @BrokerCluster(numBrokers = 1) @Name("tls") KafkaCluster tlsCluster,
                                   @BrokerCluster(numBrokers = 1) @Name("plain") KafkaCluster plainCluster,
                                   @Name("tls") Topic topic) {
        // Given: two routes — one to a TLS cluster, one to a plaintext cluster; router selects the TLS route
        var tlsClusterDef = new ClusterDefinition(TLS_CLUSTER_DEF, tlsCluster.getBootstrapServers(), upstreamTlsConfig(tlsCluster));
        var plainClusterDef = new ClusterDefinition(PLAIN_CLUSTER_DEF, plainCluster.getBootstrapServers(), null);
        var routes = List.of(
                new RouteDefinition(TLS_ROUTE, 0, List.of(), new RouteTarget(TLS_CLUSTER_DEF, null)),
                new RouteDefinition(PLAIN_ROUTE, 1, List.of(), new RouteTarget(PLAIN_CLUSTER_DEF, null)));
        var config = buildConfig(List.of(tlsClusterDef, plainClusterDef), routes, TLS_ROUTE);

        // When: produce and consume through the proxy
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer();
                var consumer = tester.consumer(
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "routing-tls-mixed-to-tls",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "k", "tls-via-mixed")))
                    .succeedsWithin(Duration.ofSeconds(10));

            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));

            // Then: proxy used TLS for the TLS route; produce/consume succeed
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("tls-via-mixed");
        }
    }

    @Test
    void plaintextRouteUsedInMixedConfig(
                                         @Tls @BrokerCluster(numBrokers = 1) @Name("tls") KafkaCluster tlsCluster,
                                         @BrokerCluster(numBrokers = 1) @Name("plain") KafkaCluster plainCluster,
                                         @Name("plain") Topic topic) {
        // Given: two routes — one to a TLS cluster, one to a plaintext cluster; router selects the plain route
        var tlsClusterDef = new ClusterDefinition(TLS_CLUSTER_DEF, tlsCluster.getBootstrapServers(), upstreamTlsConfig(tlsCluster));
        var plainClusterDef = new ClusterDefinition(PLAIN_CLUSTER_DEF, plainCluster.getBootstrapServers(), null);
        var routes = List.of(
                new RouteDefinition(TLS_ROUTE, 0, List.of(), new RouteTarget(TLS_CLUSTER_DEF, null)),
                new RouteDefinition(PLAIN_ROUTE, 1, List.of(), new RouteTarget(PLAIN_CLUSTER_DEF, null)));
        var config = buildConfig(List.of(tlsClusterDef, plainClusterDef), routes, PLAIN_ROUTE);

        // When: produce and consume through the proxy
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer();
                var consumer = tester.consumer(
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "routing-tls-mixed-to-plain",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "k", "plain-via-mixed")))
                    .succeedsWithin(Duration.ofSeconds(10));

            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));

            // Then: proxy used plaintext for the plain route; produce/consume succeed
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("plain-via-mixed");
        }
    }

    @ParameterizedTest(name = "route to {0}")
    @CsvSource({ TLS_ROUTE + "-a", TLS_ROUTE + "-b" })
    void shouldRouteToEitherTlsCluster(
                                       String selectedRoute,
                                       @Tls @BrokerCluster(numBrokers = 1) @Name("clusterA") KafkaCluster clusterA,
                                       @Tls @BrokerCluster(numBrokers = 1) @Name("clusterB") KafkaCluster clusterB)
            throws Exception {
        // Given: two TLS clusters with distinct CAs, one route per cluster
        var clusterDefA = new ClusterDefinition(TLS_CLUSTER_DEF + "-a", clusterA.getBootstrapServers(), upstreamTlsConfig(clusterA));
        var clusterDefB = new ClusterDefinition(TLS_CLUSTER_DEF + "-b", clusterB.getBootstrapServers(), upstreamTlsConfig(clusterB));
        var routes = List.of(
                new RouteDefinition(TLS_ROUTE + "-a", 0, List.of(), new RouteTarget(TLS_CLUSTER_DEF + "-a", null)),
                new RouteDefinition(TLS_ROUTE + "-b", 1, List.of(), new RouteTarget(TLS_CLUSTER_DEF + "-b", null)));
        var config = buildConfig(List.of(clusterDefA, clusterDefB), routes, selectedRoute);

        KafkaCluster expectedCluster = selectedRoute.endsWith("-a") ? clusterA : clusterB;
        KafkaCluster unexpectedCluster = selectedRoute.endsWith("-a") ? clusterB : clusterA;
        var topicName = "routing-tls-both-" + UUID.randomUUID();

        // When: produce and consume through the proxy
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var admin = tester.admin();
                var producer = tester.producer();
                var consumer = tester.consumer(
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "routing-tls-both-" + selectedRoute,
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicName, "k", "tls-both-value"))
                    .get(10, TimeUnit.SECONDS);
            consumer.subscribe(Set.of(topicName));
            var records = consumer.poll(Duration.ofSeconds(10));

            // Then: round-trip succeeds, data landed on the correct cluster
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("tls-both-value");
        }

        try (var expectedAdmin = CloseableAdmin.create(expectedCluster.getKafkaClientConfiguration());
                var unexpectedAdmin = CloseableAdmin.create(unexpectedCluster.getKafkaClientConfiguration())) {
            assertThat(expectedAdmin.listTopics().names().get(10, TimeUnit.SECONDS)).contains(topicName);
            assertThat(unexpectedAdmin.listTopics().names().get(10, TimeUnit.SECONDS)).doesNotContain(topicName);
        }
    }

    private ConfigurationBuilder buildConfig(ClusterDefinition clusterDef, List<RouteDefinition> routes, String selectedRoute) {
        return buildConfig(List.of(clusterDef), routes, selectedRoute);
    }

    private ConfigurationBuilder buildConfig(List<ClusterDefinition> clusterDefs, List<RouteDefinition> routes, String selectedRoute) {
        var routerConfig = new PassThroughRouterFactory.Config(selectedRoute);
        var routerDef = new RouterDefinition(ROUTER_NAME, PassThroughRouterFactory.class.getName(), routerConfig, routes);

        var vc = new VirtualClusterBuilder()
                .withName("routing-tls-vc")
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        var builder = baseConfigurationBuilder()
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
        clusterDefs.forEach(builder::addToClusterDefinitions);
        return builder;
    }
}
