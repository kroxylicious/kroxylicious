/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.PassThroughRouterFactory;
import io.kroxylicious.it.testplugins.SplitStaticRouterFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.ResponsePayload;
import io.kroxylicious.testing.integration.server.MockServer;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.clients.CloseableAdmin;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultGatewayBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests verifying that routing via a pass-through router
 * works identically to the direct target-cluster path.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RoutingPassThroughIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();

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
    void shouldProduceAndConsumeViaPassThroughRouter(KafkaCluster cluster, Topic topic) {
        var config = routingConfig(cluster);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer();
                var consumer = tester.consumer(
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "routing-test",
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
    void shouldListTopicsViaPassThroughRouter(KafkaCluster cluster, Topic topic) throws Exception {
        var config = routingConfig(cluster);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
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
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
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
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("routed-value");
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

    @Test
    void shouldProduceAndConsumeViaPassThroughRouterWithMultiNodeCluster(
                                                                         @BrokerCluster(numBrokers = 3) KafkaCluster cluster, Topic topic) {
        // Given: a pass-through router backed by a 3-node cluster; the partition leader
        // may be on any broker (not necessarily the bootstrap node), so the proxy must
        // resolve the correct upstream broker via the METADATA-based address registry.
        var config = routingConfig(cluster);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer();
                var consumer = tester.consumer(
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "routing-multi-node-test",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            // When: produce 10 records
            for (int i = 0; i < 10; i++) {
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "key-" + i, "value-" + i)))
                        .succeedsWithin(Duration.ofSeconds(10));
            }

            // Then: all 10 records are consumable
            consumer.subscribe(Set.of(topic.name()));
            List<ConsumerRecord<String, String>> collected = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (collected.size() < 10 && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofSeconds(1)).forEach(collected::add);
            }
            assertThat(collected).isNotEmpty().hasSize(10);
        }
    }

    @ParameterizedTest(name = "route to {0}")
    @CsvSource({ "route-a", "route-b" })
    void shouldProduceAndConsumeViaSelectedRouteWithTwoMultiNodeClusters(
                                                                         String selectedRoute,
                                                                         @BrokerCluster(numBrokers = 3) KafkaCluster clusterA,
                                                                         @BrokerCluster(numBrokers = 3) KafkaCluster clusterB)
            throws Exception {
        // Given: two 3-node clusters, two routes, a pass-through router directing all traffic to the selected route.
        // With 2 routes × 3 brokers the bijective mapping produces virtual node IDs 0-5, so the
        // gateway must expose ports for all six virtual nodes.
        var clusterDefA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var clusterDefB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);

        var routeA = new RouteDefinition("route-a", 0, List.of(), new RouteTarget("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1, List.of(), new RouteTarget("cluster-b", null));

        var routerConfig = new PassThroughRouterFactory.Config(selectedRoute);
        var routerDef = new RouterDefinition("two-cluster-router",
                PassThroughRouterFactory.class.getName(), routerConfig, List.of(routeA, routeB));

        var vc = new VirtualClusterBuilder()
                .withName("two-cluster")
                .withTarget(new RouteTarget(null, "two-cluster-router"))
                .addToGateways(defaultGatewayBuilder()
                        .withNewPortIdentifiesNode()
                        .withBootstrapAddress(HostPort.parse("localhost:9192"))
                        .withNodeIdRanges(new NamedRange("nodes", 0, 5))
                        .endPortIdentifiesNode()
                        .build())
                .build();

        var config = baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDefA)
                .addToClusterDefinitions(clusterDefB)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);

        KafkaCluster expectedCluster = selectedRoute.equals("route-a") ? clusterA : clusterB;
        KafkaCluster unexpectedCluster = selectedRoute.equals("route-a") ? clusterB : clusterA;
        var topicName = "two-cluster-multinode-" + UUID.randomUUID();

        // When: produce 10 records and consume them back through the proxy
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var admin = tester.admin();
                var producer = tester.producer();
                var consumer = tester.consumer(
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "two-cluster-multinode-test",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);

            for (int i = 0; i < 10; i++) {
                assertThat(producer.send(new ProducerRecord<>(topicName, "key-" + i, "value-" + i)))
                        .succeedsWithin(Duration.ofSeconds(10));
            }

            consumer.subscribe(Set.of(topicName));
            List<ConsumerRecord<String, String>> collected = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (collected.size() < 10 && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofSeconds(1)).forEach(collected::add);
            }
            assertThat(collected).isNotEmpty().hasSize(10);
        }

        // Then: all data is on the selected cluster, none on the other
        try (var expectedAdmin = CloseableAdmin.create(expectedCluster.getKafkaClientConfiguration());
                var unexpectedAdmin = CloseableAdmin.create(unexpectedCluster.getKafkaClientConfiguration())) {
            assertThat(expectedAdmin.listTopics().names().get(10, TimeUnit.SECONDS)).contains(topicName);
            assertThat(unexpectedAdmin.listTopics().names().get(10, TimeUnit.SECONDS)).isEmpty();
        }
    }

    @Test
    void shouldStaticallyRouteDifferentApiKeysToDifferentUpstreams() {
        // Given: two mock upstreams and a router that splits API_VERSIONS to upstream A
        // and FETCH to upstream B. Uses the low-level KafkaClient to bypass the Kafka
        // client's metadata assumptions — it just sends whatever frames we tell it to.
        try (var mockA = MockServer.startOnRandomPort();
                var mockB = MockServer.startOnRandomPort()) {

            mockA.addMockResponseForApiKey(
                    new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, new ApiVersionsResponseData()));
            mockB.addMockResponseForApiKey(
                    new ResponsePayload(ApiKeys.FETCH, (short) 12, new FetchResponseData()));

            var clusterA = new ClusterDefinition("upstream-a", "localhost:" + mockA.port(), null);
            var clusterB = new ClusterDefinition("upstream-b", "localhost:" + mockB.port(), null);
            var routeA = new RouteDefinition("route-a", 0, List.of(), new RouteTarget("upstream-a", null));
            var routeB = new RouteDefinition("route-b", 1, List.of(), new RouteTarget("upstream-b", null));

            var routerConfig = new SplitStaticRouterFactory.Config("route-a", "route-b", List.of("FETCH"));
            var routerDef = new RouterDefinition("split-router",
                    SplitStaticRouterFactory.class.getName(), routerConfig, List.of(routeA, routeB));

            var vc = new VirtualClusterBuilder()
                    .withName("split-vc")
                    .withTarget(new RouteTarget(null, "split-router"))
                    .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                    .build();

            var config = baseConfigurationBuilder()
                    .addToClusterDefinitions(clusterA)
                    .addToClusterDefinitions(clusterB)
                    .addToRouterDefinitions(routerDef)
                    .addToVirtualClusters(vc);

            try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester()) {
                // When: send API_VERSIONS (routed to upstream A) then FETCH (routed to upstream B)
                // Use separate low-level clients so each gets its own proxy session.
                try (var client = tester.simpleTestClient()) {
                    client.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "test", new ApiVersionsRequestData()));
                }
                try (var client = tester.simpleTestClient()) {
                    client.getSync(new Request(ApiKeys.FETCH, (short) 12, "test", new FetchRequestData()));
                }

                // Then: each upstream received only the API keys routed to it
                assertThat(mockA.getReceivedRequests())
                        .extracting(Request::apiKeys)
                        .containsExactly(ApiKeys.API_VERSIONS);
                assertThat(mockB.getReceivedRequests())
                        .extracting(Request::apiKeys)
                        .containsExactly(ApiKeys.FETCH);
            }
        }
    }

    @Test
    void shouldVirtualizeNodeIdsInDescribeTopicPartitionsResponse(KafkaCluster clusterA, KafkaCluster clusterB) throws Exception {
        // Given: two routes where route-b (id=1, totalRoutes=2) maps upstream broker 0
        // to virtual ID 1 + 2×0 = 1. All traffic goes to route-b.
        var clusterDefA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var clusterDefB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);
        var routeA = new RouteDefinition("route-a", 0, List.of(), new RouteTarget("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1, List.of(), new RouteTarget("cluster-b", null));
        var routerConfig = new PassThroughRouterFactory.Config("route-b");
        var routerDef = new RouterDefinition("two-cluster-router",
                PassThroughRouterFactory.class.getName(), routerConfig, List.of(routeA, routeB));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "two-cluster-router"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();
        var config = baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDefA)
                .addToClusterDefinitions(clusterDefB)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);

        var topicName = "describe-topic-parts-" + UUID.randomUUID();

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var admin = tester.admin()) {
            admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            // When
            var descriptions = admin.describeTopics(List.of(topicName)).allTopicNames().get(10, TimeUnit.SECONDS);

            // Then: leader ID is the virtual ID (1), not the upstream broker ID (0)
            assertThat(descriptions.get(topicName).partitions())
                    .singleElement()
                    .satisfies(p -> assertThat(p.leader().id()).isEqualTo(1));
        }
    }

    @Test
    void shouldRouteConnectionsToCorrectUpstreamBrokerBasedOnVirtualNodeId() {
        // Given: two mock servers representing upstream broker 0 and broker 1 of a single route.
        // With IdentityNodeIdMapping (single route), virtual node 0 = upstream broker 0 and
        // virtual node 1 = upstream broker 1, exposed on proxy ports 9193 and 9194 respectively.
        try (var mockBroker0 = MockServer.startOnRandomPort();
                var mockBroker1 = MockServer.startOnRandomPort()) {

            var apiVersionsResponse = new ApiVersionsResponseData();
            mockBroker0.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, apiVersionsResponse));
            mockBroker1.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, apiVersionsResponse));

            // METADATA from broker 0 (the route bootstrap) advertises both upstream brokers.
            // Once the proxy processes this response, the EndpointReconciler creates
            // BrokerEndpointBindings: port 9193 → mockBroker0, port 9194 → mockBroker1.
            var md = new MetadataResponseData().setControllerId(0);
            md.brokers().add(new MetadataResponseBroker().setNodeId(0).setHost("localhost").setPort(mockBroker0.port()));
            md.brokers().add(new MetadataResponseBroker().setNodeId(1).setHost("localhost").setPort(mockBroker1.port()));
            mockBroker0.addMockResponseForApiKey(new ResponsePayload(ApiKeys.METADATA, (short) 12, md));

            var clusterDef = new ClusterDefinition(TARGET_CLUSTER_NAME, "localhost:" + mockBroker0.port(), null);
            var route = new RouteDefinition(ROUTE_NAME, 0, List.of(), new RouteTarget(TARGET_CLUSTER_NAME, null));
            var routerDef = new RouterDefinition(ROUTER_NAME,
                    PassThroughRouterFactory.class.getName(), new PassThroughRouterFactory.Config(ROUTE_NAME), List.of(route));
            var vc = new VirtualClusterBuilder()
                    .withName("demo")
                    .withTarget(new RouteTarget(null, ROUTER_NAME))
                    .addToGateways(defaultGatewayBuilder()
                            .withNewPortIdentifiesNode()
                            .withBootstrapAddress(HostPort.parse("localhost:9192"))
                            .withNodeIdRanges(new NamedRange("nodes", 0, 1))
                            .endPortIdentifiesNode()
                            .build())
                    .build();
            var config = baseConfigurationBuilder()
                    .addToClusterDefinitions(clusterDef)
                    .addToRouterDefinitions(routerDef)
                    .addToVirtualClusters(vc);

            try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester()) {
                // Fetch METADATA via the bootstrap to trigger reconciliation. After getSync returns,
                // the BrokerAddressFilter has completed reconciliation so ports 9193/9194 are bound
                // to their respective upstream addresses.
                try (var bootstrap = tester.simpleTestClient()) {
                    bootstrap.getSync(new Request(ApiKeys.METADATA, (short) 12, "client", new MetadataRequestData()));
                }

                // Clear reconciliation traffic before the per-broker phase so assertions only
                // reflect requests that arrive through specific virtual-node connections.
                // Re-add the API_VERSIONS responses that were cleared along with the request history.
                mockBroker0.clear();
                mockBroker1.clear();
                mockBroker0.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, new ApiVersionsResponseData()));
                mockBroker1.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, new ApiVersionsResponseData()));

                // When: connect to virtual-node-0-port and send a request
                try (var nodeZero = tester.simpleTestClient("localhost:9193", false)) {
                    nodeZero.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));
                }

                // When: connect to virtual-node-1-port and send a request
                try (var nodeOne = tester.simpleTestClient("localhost:9194", false)) {
                    nodeOne.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));
                }
            }

            // Then: after the clear, each mock received exactly what arrived via its virtual-node port.
            assertThat(mockBroker0.getReceivedRequests())
                    .extracting(Request::apiKeys)
                    .containsExactly(ApiKeys.API_VERSIONS);

            // mockBroker1 received only API_VERSIONS from virtual-node-1; nothing from reconciliation
            assertThat(mockBroker1.getReceivedRequests())
                    .extracting(Request::apiKeys)
                    .containsExactly(ApiKeys.API_VERSIONS);
        }
    }

    @ParameterizedTest(name = "all traffic routed to {0}")
    @CsvSource({ "route-a", "route-b" })
    void shouldRouteConnectionsToCorrectUpstreamBrokerBasedOnVirtualNodeIdWithTwoRoutes(String selectedRoute) {
        // Given: two routes (route-a and route-b), each backed by two upstream brokers.
        // With BijectiveNodeIdMapping (2 routes × 2 brokers each):
        // route-a broker 0 → virtual node 0 → proxy port 9193
        // route-b broker 0 → virtual node 1 → proxy port 9194
        // route-a broker 1 → virtual node 2 → proxy port 9195
        // route-b broker 1 → virtual node 3 → proxy port 9196
        //
        // A PassThroughRouter sends ALL traffic (including METADATA) to the selected route.
        // METADATA reconciliation therefore only binds the virtual-node ports for that route:
        // route-a selected → ports 9193 and 9195 are bound to mockA0 and mockA1
        // route-b selected → ports 9194 and 9196 are bound to mockB0 and mockB1
        // The test is parameterised so each run validates one route's two broker nodes.
        try (var mockA0 = MockServer.startOnRandomPort();
                var mockA1 = MockServer.startOnRandomPort();
                var mockB0 = MockServer.startOnRandomPort();
                var mockB1 = MockServer.startOnRandomPort()) {

            // Resolve which pair of mocks and which proxy ports correspond to the selected route.
            var brokerBootstrap = "route-a".equals(selectedRoute) ? mockA0 : mockB0;
            var brokerOne = "route-a".equals(selectedRoute) ? mockA1 : mockB1;
            // route-a nodes sit on virtual nodes 0 (port +1) and 2 (port +3); route-b on 1 (+2) and 3 (+4).
            int brokerBootstrapPort = "route-a".equals(selectedRoute) ? 9193 : 9194;
            int brokerOnePort = "route-a".equals(selectedRoute) ? 9195 : 9196;

            // METADATA from the selected route's bootstrap lists both its upstream brokers.
            var metadata = new MetadataResponseData().setControllerId(0);
            metadata.brokers().add(new MetadataResponseBroker().setNodeId(0)
                    .setHost("localhost").setPort(brokerBootstrap.port()));
            metadata.brokers().add(new MetadataResponseBroker().setNodeId(1)
                    .setHost("localhost").setPort(brokerOne.port()));
            brokerBootstrap.addMockResponseForApiKey(new ResponsePayload(ApiKeys.METADATA, (short) 12, metadata));

            var clusterA = new ClusterDefinition("cluster-a", "localhost:" + mockA0.port(), null);
            var clusterB = new ClusterDefinition("cluster-b", "localhost:" + mockB0.port(), null);
            var routeA = new RouteDefinition("route-a", 0, List.of(), new RouteTarget("cluster-a", null));
            var routeB = new RouteDefinition("route-b", 1, List.of(), new RouteTarget("cluster-b", null));
            var routerDef = new RouterDefinition("two-route-two-node-router",
                    PassThroughRouterFactory.class.getName(), new PassThroughRouterFactory.Config(selectedRoute),
                    List.of(routeA, routeB));
            var vc = new VirtualClusterBuilder()
                    .withName("demo")
                    .withTarget(new RouteTarget(null, "two-route-two-node-router"))
                    .addToGateways(defaultGatewayBuilder()
                            .withNewPortIdentifiesNode()
                            .withBootstrapAddress(HostPort.parse("localhost:9192"))
                            .withNodeIdRanges(new NamedRange("nodes", 0, 3))
                            .endPortIdentifiesNode()
                            .build())
                    .build();
            var config = baseConfigurationBuilder()
                    .addToClusterDefinitions(clusterA)
                    .addToClusterDefinitions(clusterB)
                    .addToRouterDefinitions(routerDef)
                    .addToVirtualClusters(vc);

            try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester()) {
                // Send METADATA via the bootstrap to trigger reconciliation of the selected route's
                // virtual-node ports. After getSync returns, BrokerEndpointBindings are in place.
                try (var bootstrapClient = tester.simpleTestClient()) {
                    bootstrapClient.getSync(new Request(ApiKeys.METADATA, (short) 12, "client", new MetadataRequestData()));
                }

                // Clear reconciliation traffic; re-add distinct responses per broker so any
                // unexpected cross-routing shows up as a wrong key rather than an inflated count.
                brokerBootstrap.clear();
                brokerOne.clear();
                brokerBootstrap.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, new ApiVersionsResponseData()));
                brokerOne.addMockResponseForApiKey(new ResponsePayload(ApiKeys.FETCH, (short) 12, new FetchResponseData()));

                // When/Then: broker 0's virtual-node port routes to brokerBootstrap
                try (var node = tester.simpleTestClient("localhost:" + brokerBootstrapPort, false)) {
                    node.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));
                }
                assertThat(brokerBootstrap.getReceivedRequests()).extracting(Request::apiKeys)
                        .containsExactly(ApiKeys.API_VERSIONS);

                // When/Then: broker 1's virtual-node port routes to brokerOne
                try (var node = tester.simpleTestClient("localhost:" + brokerOnePort, false)) {
                    node.getSync(new Request(ApiKeys.FETCH, (short) 12, "client", new FetchRequestData()));
                }
                assertThat(brokerOne.getReceivedRequests()).extracting(Request::apiKeys)
                        .containsExactly(ApiKeys.FETCH);
            }

        }
    }
}
