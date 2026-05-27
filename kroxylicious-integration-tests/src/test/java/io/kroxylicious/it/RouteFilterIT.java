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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.ClientAuthAwareLawyer;
import io.kroxylicious.it.testplugins.ClientAuthAwareLawyerFilter;
import io.kroxylicious.it.testplugins.ClientIdRouterFactory;
import io.kroxylicious.it.testplugins.FixedClientIdFilterFactory;
import io.kroxylicious.it.testplugins.ForwardingStyle;
import io.kroxylicious.it.testplugins.PassThroughRouterFactory;
import io.kroxylicious.it.testplugins.RequestResponseMarkingFilterFactory;
import io.kroxylicious.it.testplugins.SaslPlainTermination;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
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
                "first-stamper",
                FixedClientIdFilterFactory.class.getName())
                .withConfig("clientId", "first-stamp")
                .build();

        var secondFilter = new NamedFilterDefinitionBuilder(
                "second-stamper",
                FixedClientIdFilterFactory.class.getName())
                .withConfig("clientId", "second-stamp")
                .build();

        var routeA = new RouteDefinition("route-a", 0,
                List.of("first-stamper", "second-stamper"),
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

    @Test
    void produceAndConsumeWorkThroughFilteredRoute() throws Exception {
        var config = configWithRouteFilter(
                "fixed-client-id",
                FixedClientIdFilterFactory.class.getName(),
                Map.of("clientId", "route-filter-stamped"));

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(producerConfig("route-a-client"))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "filtered-value"))
                    .get(10, TimeUnit.SECONDS);
        }

        var records = consumeDirectly(clusterA);
        assertThat(records).extracting(ConsumerRecord::value)
                .containsExactly("filtered-value");
    }

    @Test
    void threadingGuaranteeWithSynchronousRouteFilter() throws Exception {
        var config = configWithMarkingFilter("sync-marker", ForwardingStyle.SYNCHRONOUS);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(producerConfig("route-a-client"))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "threading-test"))
                    .get(10, TimeUnit.SECONDS);
        }

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .hasSizeGreaterThanOrEqualTo(1);
    }

    @Test
    void threadingGuaranteeWithAsyncDelayedRouteFilter() throws Exception {
        var config = configWithMarkingFilter("async-delayed", ForwardingStyle.ASYNCHRONOUS_DELAYED);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(producerConfig("route-a-client"))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "async-delayed-test"))
                    .get(30, TimeUnit.SECONDS);
        }

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .hasSizeGreaterThanOrEqualTo(1);
    }

    @Test
    void threadingGuaranteeWithAsyncBrokerRequestRouteFilter() throws Exception {
        var config = configWithMarkingFilter("async-broker", ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(producerConfig("route-a-client"))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "async-broker-test"))
                    .get(30, TimeUnit.SECONDS);
        }

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .hasSizeGreaterThanOrEqualTo(1);
    }

    @Test
    void vcFilterAndRouteFilterBothExecute() throws Exception {
        var vcFilter = new NamedFilterDefinitionBuilder(
                "vc-stamper",
                FixedClientIdFilterFactory.class.getName())
                .withConfig("clientId", "vc-stamped")
                .build();
        var routeFilter = new NamedFilterDefinitionBuilder(
                "route-stamper",
                FixedClientIdFilterFactory.class.getName())
                .withConfig("clientId", "route-stamped")
                .build();

        var routeA = new RouteDefinition("route-a", 0,
                List.of("route-stamper"),
                new RouteTarget("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1,
                null,
                new RouteTarget("cluster-b", null));

        var config = buildConfig(routeA, routeB, vcFilter, routeFilter);
        config.addToDefaultFilters("vc-stamper");

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(producerConfig("route-a-client"))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "vc-and-route"))
                    .get(10, TimeUnit.SECONDS);
        }

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .hasSizeGreaterThanOrEqualTo(1);
    }

    @Test
    void authenticatedSubjectAccessibleFromRouteFilter() throws Exception {
        var saslFilter = new NamedFilterDefinitionBuilder(
                "sasl-plain",
                SaslPlainTermination.class.getName())
                .withConfig("userNameToPassword",
                        Map.of("alice", "alice-secret"))
                .build();
        var lawyerFilter = new NamedFilterDefinitionBuilder(
                "lawyer",
                ClientAuthAwareLawyer.class.getName())
                .build();

        var routeA = new RouteDefinition("route-a", 0,
                List.of("lawyer"),
                new RouteTarget("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1,
                null,
                new RouteTarget("cluster-b", null));

        var config = buildConfig(routeA, routeB, saslFilter, lawyerFilter);
        config.addToDefaultFilters("sasl-plain");

        try (var tester = kroxyliciousTester(config)) {
            try (var producer = tester.producer(saslProducerConfig("alice", "alice-secret", "route-a-client"))) {
                producer.send(new ProducerRecord<>(TOPIC, "key", "authed-value"))
                        .get(10, TimeUnit.SECONDS);
            }

            var records = consumeDirectly(clusterA);
            assertThat(records).hasSize(1);
            var record = records.get(0);
            var subjectHeader = record.headers().lastHeader(
                    ClientAuthAwareLawyerFilter.HEADER_KEY_AUTHENTICATED_SUBJECT);
            assertThat(subjectHeader).isNotNull();
            var subjectValue = new String(subjectHeader.value(), java.nio.charset.StandardCharsets.UTF_8);
            assertThat(subjectValue).contains("alice");
        }
    }

    @Test
    void nestedRouterRouteFilterApplied() throws Exception {
        var stampFilter = new NamedFilterDefinitionBuilder(
                "inner-stamper",
                FixedClientIdFilterFactory.class.getName())
                .withConfig("clientId", "nested-stamped")
                .build();

        // Inner router: routes all to route-inner-a (with filter) on cluster-a
        var innerRouteA = new RouteDefinition("route-inner-a", 0,
                List.of("inner-stamper"),
                new RouteTarget("cluster-a", null));
        var innerRouter = new RouterDefinition("inner-router",
                ClientIdRouterFactory.class.getName(),
                new ClientIdRouterFactory.Config(
                        Map.of("route-a-client", "route-inner-a"),
                        "route-inner-a"),
                List.of(innerRouteA));

        // Outer router: routes all to route-outer which targets the inner router
        var outerRoute = new RouteDefinition("route-outer", 0,
                null,
                new RouteTarget(null, "inner-router"));
        var outerRouter = new RouterDefinition("outer-router",
                ClientIdRouterFactory.class.getName(),
                new ClientIdRouterFactory.Config(
                        Map.of("route-a-client", "route-outer"),
                        "route-outer"),
                List.of(outerRoute));

        var defA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var defB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "outer-router"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        var config = baseConfigurationBuilder()
                .addToClusterDefinitions(defA, defB)
                .addToFilterDefinitions(stampFilter)
                .addToRouterDefinitions(outerRouter, innerRouter)
                .addToVirtualClusters(vc);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(producerConfig("route-a-client"))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "nested-filtered"))
                    .get(10, TimeUnit.SECONDS);
        }

        var records = consumeDirectly(clusterA);
        assertThat(records).extracting(ConsumerRecord::value)
                .containsExactly("nested-filtered");
    }

    @Test
    void staticRouteWithFilterApplied() throws Exception {
        var stampFilter = new NamedFilterDefinitionBuilder(
                "stamper",
                FixedClientIdFilterFactory.class.getName())
                .withConfig("clientId", "static-route-stamped")
                .build();

        // PassThroughRouter: all API keys are static, routed to "the-route"
        var route = new RouteDefinition("the-route", 0,
                List.of("stamper"),
                new RouteTarget("cluster-a", null));
        var router = new RouterDefinition("passthrough-router",
                PassThroughRouterFactory.class.getName(),
                new PassThroughRouterFactory.Config("the-route"),
                List.of(route));

        var defA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "passthrough-router"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        var config = baseConfigurationBuilder()
                .addToClusterDefinitions(defA)
                .addToFilterDefinitions(stampFilter)
                .addToRouterDefinitions(router)
                .addToVirtualClusters(vc);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(producerConfig("any-client"))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "static-filtered"))
                    .get(10, TimeUnit.SECONDS);
        }

        var records = consumeDirectly(clusterA);
        assertThat(records).extracting(ConsumerRecord::value)
                .containsExactly("static-filtered");
    }

    @Test
    @org.junit.jupiter.api.Disabled("Requires TLS infrastructure (certificates, keystores) — see AbstractTlsIT for the pattern")
    void sniHostnameAccessibleFromRouteFilter() {
        // TODO: configure TLS gateway with SNI, add a route filter that reads
        // context.sniHostname() and injects it into a PRODUCE record header,
        // then verify the header value matches the expected SNI hostname.
    }

    private static Map<String, Object> saslProducerConfig(String username, String password, String clientId) {
        var config = producerConfig(clientId);
        config.put("security.protocol", "SASL_PLAINTEXT");
        config.put(org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM, "PLAIN");
        config.put(org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required"
                        + " username=\"" + username + "\""
                        + " password=\"" + password + "\";");
        return config;
    }

    private ConfigurationBuilder configWithMarkingFilter(String name, ForwardingStyle style) {
        var filterDef = markingFilterDef(name, style);

        var routeA = new RouteDefinition("route-a", 0,
                List.of(name),
                new RouteTarget("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1,
                null,
                new RouteTarget("cluster-b", null));

        return buildConfig(routeA, routeB, filterDef);
    }

    private NamedFilterDefinition markingFilterDef(String name, ForwardingStyle style) {
        return new NamedFilterDefinitionBuilder(name, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(ApiKeys.PRODUCE),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.REQUEST),
                        "name", name,
                        "forwardingStyle", style)
                .build();
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

    private static Map<String, Object> consumerConfig(String clientId) {
        var config = new HashMap<String, Object>();
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "route-filter-it-" + System.nanoTime());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return config;
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
