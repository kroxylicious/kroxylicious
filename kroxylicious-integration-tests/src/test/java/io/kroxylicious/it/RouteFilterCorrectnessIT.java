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

import org.apache.kafka.clients.producer.ProducerRecord;
import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.message.ListGroupsResponseData;
import io.kroxylicious.kafka.common.message.ListTransactionsRequestData;
import io.kroxylicious.kafka.common.message.ListTransactionsResponseData;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.ForwardingStyle;
import io.kroxylicious.it.testplugins.RequestCountingFilter;
import io.kroxylicious.it.testplugins.RequestCountingFilterFactory;
import io.kroxylicious.it.testplugins.RequestResponseMarkingFilter;
import io.kroxylicious.it.testplugins.RequestResponseMarkingFilterFactory;
import io.kroxylicious.it.testplugins.router.ContextCapturingRouterFactory;
import io.kroxylicious.it.testplugins.router.DynamicProduceRouterFactory;
import io.kroxylicious.it.testplugins.router.PassThroughRouterFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.testing.filter.record.RecordTestUtils;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.ResponsePayload;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.it.UnknownTaggedFields.unknownTaggedFieldsToStrings;
import static io.kroxylicious.it.testplugins.RequestResponseMarkingFilter.FILTER_NAME_TAG;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.FETCH;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.LIST_GROUPS;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.LIST_TRANSACTIONS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies per-route filter correctness across a range of user-facing gestures.
 *
 * <p>C1: Route filters see all traffic on a route, including router-originated frames.
 * <p>C2: Routing stays stable across many dynamic-produce operations.
 * <p>C3: {@code sendRequest()} from a route filter reaches the correct backend.
 * <p>C5: Route filters process only their own route's {@code InternalRequestFrame}s.
 * <p>C6: Same filter factory type on multiple routes does not cause cross-contamination.
 * <p>C7: Route filter {@code onResponse} is called for dynamically-dispatched responses.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RouteFilterCorrectnessIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();
    private static final String ROUTER_NAME = "router";
    private static final String CLUSTER_NAME = "backing";
    private static final String ROUTE_A = "route-a";
    private static final String ROUTE_B = "route-b";

    @BeforeEach
    void resetRouterState() {
        ContextCapturingRouterFactory.reset();
    }

    // ---------------------------------------------------------------------------
    // C1: Route filters see all traffic on a route, including router-originated frames
    // ---------------------------------------------------------------------------

    /**
     * When a router uses dynamic routing, it forwards client requests to the backend via
     * {@code RouterContext.sendRequest()}. Route filters apply to all traffic on a route,
     * including these router-originated frames. A name-mapping filter on a route must
     * transform topic names in all requests reaching the backend through that route.
     */
    @Test
    void routeFilterSeesRouterOriginatedTraffic(KafkaCluster cluster, Topic topic) throws Exception {
        String counterId = "c1-" + topic.name();
        RequestCountingFilter.reset(counterId);

        // Given
        var filterDef = new NamedFilterDefinitionBuilder("counter", RequestCountingFilterFactory.class.getName())
                .withConfig("counterId", counterId)
                .build();
        var clusterDef = new ClusterDefinition(CLUSTER_NAME, cluster.getBootstrapServers(), null);
        var route = new RouteDefinition(ROUTE_A, 0, List.of("counter"), new RouteTarget(CLUSTER_NAME, null));
        var routerDef = new RouterDefinition(ROUTER_NAME, DynamicProduceRouterFactory.class.getName(),
                new DynamicProduceRouterFactory.Config(ROUTE_A), List.of(route));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();
        var config = baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToFilterDefinitions(filterDef)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000))) {

            // When
            producer.send(new ProducerRecord<>(topic.name(), "key", "value")).get();
        }

        // Then
        assertThat(RequestCountingFilter.countFor(counterId, ApiKeys.PRODUCE))
                .as("route filter should see the router's PRODUCE forwarded via sendRequest()")
                .isGreaterThanOrEqualTo(1);
    }

    // ---------------------------------------------------------------------------
    // C2: Routing stability across many dynamic-produce operations
    // ---------------------------------------------------------------------------

    /**
     * When a router uses dynamic routing across many operations, routing must remain stable
     * and deliver all records. Exposed a bug where correlation ID map entries leaked for
     * router-internal requests.
     */
    @Test
    void routingRemainsStableAfterManyDynamicOperations(KafkaCluster cluster, Topic topic) throws Exception {
        // Given
        var clusterDef = new ClusterDefinition(CLUSTER_NAME, cluster.getBootstrapServers(), null);
        var route = new RouteDefinition(ROUTE_A, 0, List.of(), new RouteTarget(CLUSTER_NAME, null));
        var routerDef = new RouterDefinition(ROUTER_NAME, DynamicProduceRouterFactory.class.getName(),
                new DynamicProduceRouterFactory.Config(ROUTE_A), List.of(route));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();
        var config = baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
                var consumer = tester.consumer(Map.of(GROUP_ID_CONFIG, "c2-stability", AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            // When
            for (int i = 0; i < 50; i++) {
                producer.send(new ProducerRecord<>(topic.name(), "key", "value-" + i)).get();
            }

            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(30));

            // Then
            assertThat(records).hasSize(50);
        }
    }

    // ---------------------------------------------------------------------------
    // C3: Route filter sendRequest() reaches the correct backend
    // ---------------------------------------------------------------------------

    /**
     * When a route filter calls {@code FilterContext.sendRequest()}, the OOB request must reach
     * the correct backend and the filter must observe the response. Exposed a bug where the
     * backend was chosen non-deterministically with multiple active routes.
     */
    @Test
    void routeFilterSendRequestWorksWithSingleRoute() {
        var filterName = "c3-marker";

        // Given
        var filterDef = new NamedFilterDefinitionBuilder(filterName, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(LIST_TRANSACTIONS),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.REQUEST),
                        "name", filterName,
                        "forwardingStyle", ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER)
                .build();

        try (var tester = KroxyliciousTesters.mockKafkaKroxyliciousTester(
                mockBootstrap -> singleRouteConfig(mockBootstrap, filterDef), ROUTING_ENABLED);
                var client = tester.simpleTestClient()) {

            ApiVersionsResponseData apiVersions = new ApiVersionsResponseData();
            apiVersions.apiKeys().add(new ApiVersionsResponseData.ApiVersion()
                    .setApiKey(FETCH.id).setMaxVersion(FETCH.latestVersion()).setMinVersion(FETCH.oldestVersion()));
            tester.addMockResponseForApiKey(new ResponsePayload(API_VERSIONS, API_VERSIONS.latestVersion(), apiVersions));
            tester.addMockResponseForApiKey(new ResponsePayload(LIST_TRANSACTIONS, LIST_TRANSACTIONS.latestVersion(), new ListTransactionsResponseData()));
            tester.addMockResponseForApiKey(new ResponsePayload(LIST_GROUPS, LIST_GROUPS.latestVersion(), new ListGroupsResponseData()));

            // When
            client.getSync(new Request(LIST_TRANSACTIONS, LIST_TRANSACTIONS.latestVersion(), "client", new ListTransactionsRequestData()));

            // Then
            var requestAtBroker = tester.getOnlyRequestForApiKey(LIST_TRANSACTIONS).message();
            assertThat(unknownTaggedFieldsToStrings(requestAtBroker, FILTER_NAME_TAG))
                    .as("filter must have marked the request, proving sendRequest() reached the mock backend")
                    .containsExactly(RequestResponseMarkingFilter.class.getSimpleName() + "-" + filterName + "-request");
        }
    }

    /**
     * When a filter on route-a sends an internal request, filters on route-b must not process it.
     * Exposed a bug where {@code RouteFilterHandler} passed all {@code InternalRequestFrame}s
     * to downstream handlers without checking route ownership.
     */
    @Test
    void routeFilterDoesNotSeeInternalRequestsOriginatedByOtherRouteFilter(KafkaCluster cluster, Topic topic) throws Exception {
        String counterId = "c5-" + topic.name();
        RequestCountingFilter.reset(counterId);
        var markerName = "c5-marker";

        // Given
        var markingFilterDef = new NamedFilterDefinitionBuilder(markerName, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(ApiKeys.PRODUCE),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.REQUEST),
                        "name", markerName,
                        "forwardingStyle", ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER)
                .build();
        var countingFilterDef = new NamedFilterDefinitionBuilder("c5-counter", RequestCountingFilterFactory.class.getName())
                .withConfig("counterId", counterId)
                .build();
        var config = twoRouteConfig(cluster.getBootstrapServers(),
                List.of(markerName), List.of("c5-counter"),
                List.of(markingFilterDef, countingFilterDef));

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000))) {

            // When
            producer.send(new ProducerRecord<>(topic.name(), "key", "value")).get();
        }

        // Then
        assertThat(RequestCountingFilter.countFor(counterId, ApiKeys.LIST_GROUPS))
                .as("route-b filter must not process InternalRequestFrame from route-a's filter")
                .isZero();
    }

    // ---------------------------------------------------------------------------
    // C6: Same filter factory type on multiple routes does not cause cross-contamination
    // ---------------------------------------------------------------------------

    /**
     * When the same filter factory type appears on multiple routes, each route gets its own
     * filter instance. Internal requests from one route must not reach the other route's filters,
     * even when both routes share the same factory type.
     */
    @Test
    void sameFilterDefinitionOnBothRoutesDoesNotCrossContaminate(KafkaCluster cluster, Topic topic) throws Exception {
        String counterIdA = "c6-route-a-" + topic.name();
        String counterIdB = "c6-route-b-" + topic.name();
        RequestCountingFilter.reset(counterIdA);
        RequestCountingFilter.reset(counterIdB);
        var markerName = "c6-marker";

        // Given
        var markingFilterDef = new NamedFilterDefinitionBuilder(markerName, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(ApiKeys.PRODUCE),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.REQUEST),
                        "name", markerName,
                        "forwardingStyle", ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER)
                .build();
        var counterADef = new NamedFilterDefinitionBuilder("c6-counter-a", RequestCountingFilterFactory.class.getName())
                .withConfig("counterId", counterIdA)
                .build();
        var counterBDef = new NamedFilterDefinitionBuilder("c6-counter-b", RequestCountingFilterFactory.class.getName())
                .withConfig("counterId", counterIdB)
                .build();
        var config = twoRouteConfig(cluster.getBootstrapServers(),
                List.of(markerName, "c6-counter-a"), List.of("c6-counter-b"),
                List.of(markingFilterDef, counterADef, counterBDef));

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000))) {

            // When
            producer.send(new ProducerRecord<>(topic.name(), "key", "value")).get();
        }

        // Then
        assertThat(RequestCountingFilter.countFor(counterIdA, ApiKeys.LIST_GROUPS))
                .as("route-a's counting filter should see the internal LIST_GROUPS from the marking filter on the same route")
                .isPositive();
        assertThat(RequestCountingFilter.countFor(counterIdB, ApiKeys.LIST_GROUPS))
                .as("route-b's counting filter must not see internal LIST_GROUPS from route-a's filter, even though it uses the same factory type")
                .isZero();
    }

    // ---------------------------------------------------------------------------
    // C7: Route filter onResponse called for dynamically-dispatched responses
    // ---------------------------------------------------------------------------

    /**
     * When a router uses dynamic dispatch, the upstream response flows back through the route
     * filter chain and the filter's {@code onResponse} must be called. Exposed a bug where
     * routing-range correlation IDs were excluded from {@code correlationIdToRoute}, causing
     * responses to skip {@code onResponse} silently.
     */
    @Test
    void routeFilterOnResponseCalledForDynamicallyDispatchedResponse(KafkaCluster cluster, Topic topic) {
        var filterName = "c7-filter";

        // Given
        var filterDef = new NamedFilterDefinitionBuilder(filterName, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(ApiKeys.PRODUCE),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.RESPONSE),
                        "name", filterName,
                        "forwardingStyle", ForwardingStyle.SYNCHRONOUS)
                .build();
        var config = contextCapturingConfigWithRouteFilter(cluster.getBootstrapServers(), filterDef);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var client = tester.simpleTestClient()) {

            // Given
            client.getSync(new Request(ApiKeys.METADATA, (short) 12, "metadata-client", new MetadataRequestData()));

            // When
            var response = client.getSync(produceRequest(topic.name(), (short) 1));

            // Then
            assertThat(unknownTaggedFieldsToStrings(response.payload().message(), FILTER_NAME_TAG))
                    .as("route filter onResponse must be invoked for dynamically-dispatched PRODUCE responses")
                    .containsExactly(RequestResponseMarkingFilter.class.getSimpleName() + "-" + filterName + "-response");
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private ConfigurationBuilder contextCapturingConfigWithRouteFilter(String bootstrapServers, NamedFilterDefinition filterDef) {
        var clusterDef = new ClusterDefinition(CLUSTER_NAME, bootstrapServers, null);
        var route = new RouteDefinition(ROUTE_A, 0, List.of(filterDef.name()), new RouteTarget(CLUSTER_NAME, null));
        var routerDef = new RouterDefinition(ROUTER_NAME, ContextCapturingRouterFactory.class.getName(),
                new ContextCapturingRouterFactory.Config(ROUTE_A), List.of(route));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();
        return baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToFilterDefinitions(filterDef)
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

    private ConfigurationBuilder twoRouteConfig(String bootstrapServers,
                                                List<String> routeAFilterNames,
                                                List<String> routeBFilterNames,
                                                List<NamedFilterDefinition> filterDefs) {
        var clusterDef = new ClusterDefinition(CLUSTER_NAME, bootstrapServers, null);
        var routeA = new RouteDefinition(ROUTE_A, 0, routeAFilterNames, new RouteTarget(CLUSTER_NAME, null));
        var routeB = new RouteDefinition(ROUTE_B, 1, routeBFilterNames, new RouteTarget(CLUSTER_NAME, null));
        var routerDef = new RouterDefinition(ROUTER_NAME, PassThroughRouterFactory.class.getName(),
                new PassThroughRouterFactory.Config(ROUTE_A), List.of(routeA, routeB));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();
        var builder = baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
        for (var fd : filterDefs) {
            builder.addToFilterDefinitions(fd);
        }
        return builder;
    }

    private ConfigurationBuilder singleRouteConfig(String bootstrapServers, NamedFilterDefinition filterDef) {
        var clusterDef = new ClusterDefinition(CLUSTER_NAME, bootstrapServers, null);
        var route = new RouteDefinition(ROUTE_A, 0, List.of(filterDef.name()), new RouteTarget(CLUSTER_NAME, null));
        var routerDef = new RouterDefinition(ROUTER_NAME, PassThroughRouterFactory.class.getName(),
                new PassThroughRouterFactory.Config(ROUTE_A), List.of(route));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();
        return baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToFilterDefinitions(filterDef)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }
}
