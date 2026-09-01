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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.kroxylicious.kafka.common.message.ListGroupsResponseData;
import io.kroxylicious.kafka.common.message.ListTransactionsRequestData;
import io.kroxylicious.kafka.common.message.ListTransactionsResponseData;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.ProduceResponseData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.Errors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.ForwardingStyle;
import io.kroxylicious.it.testplugins.RequestResponseMarkingFilterFactory;
import io.kroxylicious.it.testplugins.router.ContextCapturingRouterFactory;
import io.kroxylicious.it.testplugins.router.DynamicProduceRouterFactory;
import io.kroxylicious.it.testplugins.router.PassThroughRouterFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.filter.record.RecordTestUtils;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.ResponsePayload;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.OS_ASSIGNED_BOOTSTRAP;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultGatewayBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.LIST_GROUPS;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.LIST_TRANSACTIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests verifying that dynamic routing ({@code Router.onRequest()})
 * works correctly end-to-end: requests are deserialised, passed to the router,
 * forwarded to the backend via {@code sendRequest()}, and the response is
 * delivered to the client via {@code respondWith()}.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RoutingDynamicIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();

    private static final String ROUTE_NAME = "default-route";
    private static final String ROUTER_NAME = "dynamic-produce";
    private static final String TARGET_CLUSTER_NAME = "backing";

    @BeforeEach
    void resetRouterState() {
        ContextCapturingRouterFactory.reset();
    }

    private ConfigurationBuilder dynamicRoutingConfig(KafkaCluster cluster) {
        var targetCluster = new ClusterDefinition(TARGET_CLUSTER_NAME, cluster.getBootstrapServers(), null);
        var route = new RouteDefinition(ROUTE_NAME, 0, List.of(), new RouteTarget(TARGET_CLUSTER_NAME, null));
        var routerConfig = new DynamicProduceRouterFactory.Config(ROUTE_NAME);
        var routerDef = new RouterDefinition(ROUTER_NAME,
                DynamicProduceRouterFactory.class.getName(), routerConfig, List.of(route));

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
    @SuppressWarnings("FutureReturnValueIgnored") // acks=0 fire-and-forget: send() returns before broker acknowledgement; see producer config in the try block below
    void shouldForwardFireAndForgetProduceToUpstream(KafkaCluster cluster, Topic topic) {
        // Given
        var config = dynamicRoutingConfig(cluster);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of("acks", "0", "retries", "0", "linger.ms", "0"));
                var consumer = tester.consumer(
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "fire-and-forget-upstream-test",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            // When: acks=0 produce — send() returns before any broker acknowledgement
            producer.send(new ProducerRecord<>(topic.name(), "key", "value"));
            producer.flush();

            // Then: record arrives at the upstream Kafka cluster
            consumer.subscribe(Set.of(topic.name()));
            assertThat(consumer.poll(Duration.ofSeconds(10)).iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("value");
        }
    }

    @Test
    void shouldFireAndForgetProduceNotBlockSubsequentResponses(KafkaCluster cluster, Topic topic) {
        // Given
        var config = dynamicRoutingConfig(cluster);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var client = tester.simpleTestClient()) {

            // Establish the upstream route before sending PRODUCE requests
            // (METADATA is statically routed so it does not consume a response-sequencer slot)
            client.getSync(new Request(ApiKeys.METADATA, (short) 12, "test", new MetadataRequestData()));

            // When: fire-and-forget PRODUCE (acks=0) — dynamically routed, no response sent to client
            var fireAndForget = client.get(produceRequest(topic.name(), (short) 0));
            assertThat(fireAndForget).succeedsWithin(Duration.ofSeconds(5)).isNull();

            // Then: subsequent PRODUCE (acks=1) receives its response — sequencer was not blocked
            var withAck = client.getSync(produceRequest(topic.name(), (short) 1));
            assertThat(withAck.payload().message()).isInstanceOf(ProduceResponseData.class);
            var produceResponse = (ProduceResponseData) withAck.payload().message();
            assertThat(produceResponse.responses()).singleElement()
                    .satisfies(r -> assertThat(r.partitionResponses()).singleElement()
                            .satisfies(p -> assertThat(p.errorCode()).isEqualTo(Errors.NONE.code())));
        }
    }

    /**
     * A VC-level filter calls {@code FilterContext.sendRequest()} inside {@code onRequest}
     * for PRODUCE. The OOB {@code InternalRequestFrame} reaches {@code RouterDispatchHandler}
     * and enters {@code dispatchDynamically}. The router's response must complete the filter
     * promise rather than being dispatched to {@code onResponse}, so that PRODUCE can proceed.
     */
    @Test
    void vcFilterOobCompletesWhenApiKeyDynamicallyRouted(KafkaCluster cluster, Topic topic) {
        var filterName = "oob-filter";

        // Given: VC-level filter that sends an OOB from onRequest for PRODUCE
        var filterDef = new NamedFilterDefinitionBuilder(filterName, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(ApiKeys.PRODUCE),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.REQUEST),
                        "name", filterName,
                        "forwardingStyle", ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER)
                .build();
        var config = contextCapturingConfigWithVcFilter(cluster, filterDef);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer()) {

            // When
            var send = producer.send(new ProducerRecord<>(topic.name(), "key", "value"));

            // Then: the OOB must complete so PRODUCE can reach the broker
            assertThat(send)
                    .as("PRODUCE must succeed: VC filter OOB must complete when API key is dynamically routed")
                    .succeedsWithin(Duration.ofSeconds(10));
        }
    }

    /**
     * When a client connects to a per-node port before the proxy has learned the upstream
     * topology, {@code EagerMetadataLearner} fires an OOB METADATA request. The OOB
     * response must complete the filter promise so the connection closes and the client can
     * reconnect to the correct broker.
     */
    @Test
    void eagerMetadataLearnerOobCompletesWhenApiKeyDynamicallyRouted(KafkaCluster cluster, Topic topic) {
        // Given
        var config = contextCapturingConfigWithNodePort(cluster);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester()) {

            // When: first connection to per-node port triggers EagerMetadataLearner OOB
            var firstFuture = tester.simpleTestClient("localhost:9193", false)
                    .get(new Request(ApiKeys.METADATA, (short) 12, "init", new MetadataRequestData()));

            // Then: EagerMetadataLearner must receive the METADATA response and close
            assertThat(firstFuture)
                    .as("EagerMetadataLearner OOB METADATA must complete so the first connection returns and closes")
                    .succeedsWithin(Duration.ofSeconds(5));
            assertThat(firstFuture.toCompletableFuture().join().payload().message())
                    .isInstanceOf(MetadataResponseData.class);

            // When: second connection to the same port, upstream topology now known
            try (var secondClient = tester.simpleTestClient("localhost:9193", false)) {
                var response = secondClient.getSync(produceRequest(topic.name(), (short) 1));

                // Then: PRODUCE succeeds normally on the second connection
                assertThat(response.payload().message()).isInstanceOf(ProduceResponseData.class);
                var produceResponse = (ProduceResponseData) response.payload().message();
                assertThat(produceResponse.responses())
                        .singleElement()
                        .satisfies(r -> assertThat(r.partitionResponses()).singleElement()
                                .satisfies(p -> assertThat(p.errorCode()).isEqualTo(Errors.NONE.code())));
            }
        }
    }

    /**
     * When {@code Router.onRequest()} throws synchronously (rather than returning a failed
     * {@code CompletionStage}), the runtime must still close the connection per
     * {@code RouterContext}'s documented contract, rather than leaving it open with the request
     * unanswered forever.
     */
    @Test
    void regularRequestShouldCloseConnectionWhenRouterThrowsSynchronously(KafkaCluster cluster) {
        // Given
        var clusterDef = new ClusterDefinition(TARGET_CLUSTER_NAME, cluster.getBootstrapServers(), null);
        var route = new RouteDefinition(ROUTE_NAME, 0, List.of(), new RouteTarget(TARGET_CLUSTER_NAME, null));
        var routerDef = new RouterDefinition(ROUTER_NAME, ContextCapturingRouterFactory.class.getName(),
                new ContextCapturingRouterFactory.Config(ROUTE_NAME), List.of(route));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();
        var config = baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
        ContextCapturingRouterFactory.currentAction.set((apiKey, apiVersion, header, request, ctx) -> {
            throw new RuntimeException("boom");
        });

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var client = tester.simpleTestClient()) {

            // When
            var future = client.get(new Request(ApiKeys.METADATA, (short) 12, "client", new MetadataRequestData()));

            // Then: the request must not hang forever, and the connection must actually be closed
            // (not merely that the future failed for some other reason)
            assertThat(future).failsWithin(Duration.ofSeconds(10));
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(client.isOpen()).isFalse());
        }
    }

    /**
     * Same as above, but the throw happens while completing a VC-level filter's out-of-band
     * request rather than the client's own request.
     */
    @Test
    void vcFilterOobShouldCloseConnectionWhenRouterThrowsSynchronously(KafkaCluster cluster, Topic topic) {
        var filterName = "oob-filter";

        // Given
        var filterDef = new NamedFilterDefinitionBuilder(filterName, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(ApiKeys.PRODUCE),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.REQUEST),
                        "name", filterName,
                        "forwardingStyle", ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER)
                .build();
        var config = contextCapturingConfigWithVcFilter(cluster, filterDef);
        ContextCapturingRouterFactory.currentAction.set((apiKey, apiVersion, header, request, ctx) -> {
            throw new RuntimeException("boom");
        });

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var client = tester.simpleTestClient()) {

            // When
            var future = client.get(produceRequest(topic.name(), (short) 1));

            // Then: the request must not hang forever, and the connection must actually be closed
            // (not merely that the future failed for some other reason)
            assertThat(future).failsWithin(Duration.ofSeconds(10));
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(client.isOpen()).isFalse());
        }
    }

    /**
     * Same failure mode as above, but the throwing router is a nested one, reached via an
     * outer-route filter's out-of-band request. Exercises {@code handleNestedOobCompletion}'s
     * exception handling specifically, rather than the top-level {@code handleOobCompletion}.
     *
     * <p>Topology:
     * <pre>
     * Client → demo VC
     *   → outer router (PassThrough → "to-inner")
     *   → outer route "to-inner" [RequestResponseMarkingFilter, sends OOB LIST_GROUPS]
     *   → inner router (ContextCapturing, onRequest throws)
     *   → inner route "backend"
     *   → mock server
     * </pre>
     */
    @Test
    void nestedOobShouldCloseConnectionWhenRouterThrowsSynchronously() {
        // Given
        var filterName = "outer-oob-filter";
        var filterDef = new NamedFilterDefinitionBuilder(filterName, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(LIST_TRANSACTIONS),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.REQUEST),
                        "name", filterName,
                        "forwardingStyle", ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER)
                .build();
        var innerRoute = new RouteDefinition("backend", 0, List.of(), new RouteTarget("mock-cluster", null));
        var innerRouter = new RouterDefinition("inner", ContextCapturingRouterFactory.class.getName(),
                new ContextCapturingRouterFactory.Config("backend"), List.of(innerRoute));
        var outerRoute = new RouteDefinition("to-inner", 0, List.of(filterName), new RouteTarget(null, "inner"));
        var outerRouter = new RouterDefinition("outer", PassThroughRouterFactory.class.getName(),
                new PassThroughRouterFactory.Config("to-inner"), List.of(outerRoute));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "outer"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(OS_ASSIGNED_BOOTSTRAP).build())
                .build();

        ContextCapturingRouterFactory.currentAction.set((apiKey, apiVersion, header, request, ctx) -> {
            throw new RuntimeException("boom");
        });

        try (var tester = KroxyliciousTesters.mockKafkaKroxyliciousTester(s -> {
            var clusterDef = new ClusterDefinition("mock-cluster", s, null);
            return baseConfigurationBuilder()
                    .addToClusterDefinitions(clusterDef)
                    .addToFilterDefinitions(filterDef)
                    .addToRouterDefinitions(outerRouter, innerRouter)
                    .addToVirtualClusters(vc);
        }, ROUTING_ENABLED);
                var client = tester.simpleTestClient()) {

            tester.addMockResponseForApiKey(new ResponsePayload(LIST_TRANSACTIONS, LIST_TRANSACTIONS.latestVersion(), new ListTransactionsResponseData()));
            tester.addMockResponseForApiKey(new ResponsePayload(LIST_GROUPS, LIST_GROUPS.latestVersion(), new ListGroupsResponseData()));

            // When
            var future = client.get(new Request(LIST_TRANSACTIONS, LIST_TRANSACTIONS.latestVersion(), "client", new ListTransactionsRequestData()));

            // Then: the request must not hang forever — the connection closes and the future fails
            assertThat(future).failsWithin(Duration.ofSeconds(10));
        }
    }

    private ConfigurationBuilder contextCapturingConfigWithVcFilter(KafkaCluster cluster, NamedFilterDefinition filterDef) {
        var clusterDef = new ClusterDefinition(TARGET_CLUSTER_NAME, cluster.getBootstrapServers(), null);
        var route = new RouteDefinition(ROUTE_NAME, 0, List.of(), new RouteTarget(TARGET_CLUSTER_NAME, null));
        var routerDef = new RouterDefinition(ROUTER_NAME, ContextCapturingRouterFactory.class.getName(),
                new ContextCapturingRouterFactory.Config(ROUTE_NAME), List.of(route));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .withFilters(List.of(filterDef.name()))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();
        return baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToFilterDefinitions(filterDef)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }

    private ConfigurationBuilder contextCapturingConfigWithNodePort(KafkaCluster cluster) {
        var clusterDef = new ClusterDefinition(TARGET_CLUSTER_NAME, cluster.getBootstrapServers(), null);
        var route = new RouteDefinition(ROUTE_NAME, 0, List.of(), new RouteTarget(TARGET_CLUSTER_NAME, null));
        var routerDef = new RouterDefinition(ROUTER_NAME, ContextCapturingRouterFactory.class.getName(),
                new ContextCapturingRouterFactory.Config(ROUTE_NAME), List.of(route));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .addToGateways(defaultGatewayBuilder()
                        .withNewPortIdentifiesNode()
                        .withBootstrapAddress(HostPort.parse("localhost:9192"))
                        .withNodeIdRanges(new NamedRange("nodes", 0, 0))
                        .endPortIdentifiesNode()
                        .build())
                .build();
        return baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }

    private static Request produceRequest(String topicName, short acks) {
        var records = RecordTestUtils.singleElementMemoryRecords("key", "value");
        var partitionData = new ProduceRequestData.PartitionProduceData()
                .setIndex(0)
                .setRecords(records);
        var topicData = new ProduceRequestData.TopicProduceData()
                .setName(topicName)
                .setPartitionData(List.of(partitionData));
        var topicCollection = new ProduceRequestData.TopicProduceDataCollection(
                List.of(topicData).iterator());
        var produceData = new ProduceRequestData()
                .setAcks(acks)
                .setTimeoutMs(5000)
                .setTopicData(topicCollection);
        return new Request(ApiKeys.PRODUCE, (short) 9, "test-client", produceData);
    }

    @Test
    void shouldProduceAndConsumeViaDynamicRouting(KafkaCluster cluster, Topic topic) {
        // Given
        var config = dynamicRoutingConfig(cluster);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer();
                var consumer = tester.consumer(
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "dynamic-routing-test",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            // When
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "key", "value")))
                    .succeedsWithin(Duration.ofSeconds(10));

            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));

            // Then
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("value");
        }
    }
}
