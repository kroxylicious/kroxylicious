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
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.CorrelationIdCollectingFilter;
import io.kroxylicious.it.testplugins.CorrelationIdCollectingFilterFactory;
import io.kroxylicious.it.testplugins.ForwardingStyle;
import io.kroxylicious.it.testplugins.RequestCountingFilter;
import io.kroxylicious.it.testplugins.RequestCountingFilterFactory;
import io.kroxylicious.it.testplugins.RequestResponseMarkingFilter;
import io.kroxylicious.it.testplugins.RequestResponseMarkingFilterFactory;
import io.kroxylicious.it.testplugins.ResponseCountingFilter;
import io.kroxylicious.it.testplugins.ResponseCountingFilterFactory;
import io.kroxylicious.it.testplugins.router.ContextCapturingRouterFactory;
import io.kroxylicious.it.testplugins.router.DynamicProduceRouterFactory;
import io.kroxylicious.it.testplugins.router.FanOutRouterFactory;
import io.kroxylicious.it.testplugins.router.PassThroughRouterFactory;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.testing.filter.record.KafkaRecordTestUtils;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.ResponsePayload;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.it.UnknownTaggedFields.unknownTaggedFieldsToStrings;
import static io.kroxylicious.it.testplugins.RequestResponseMarkingFilter.FILTER_NAME_TAG;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.LIST_GROUPS;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.apache.kafka.common.protocol.ApiKeys.FETCH;
import static org.apache.kafka.common.protocol.ApiKeys.LIST_TRANSACTIONS;
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
 * <p>C8: Sibling filters' {@code onResponse} observation of an out-of-band response is not
 *     corrupted when two or more routes have concurrent out-of-band requests in flight.
 * <p>C10: An outer route's filter observes a response completed via a nested router's own dynamic
 *     dispatch, the same way it observes that nested route's requests.
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
        assertThat(RequestCountingFilter.countFor(counterId, io.kroxylicious.kafka.common.protocol.ApiKeys.PRODUCE))
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
            tester.addMockResponseForApiKey(
                    new ResponsePayload(org.apache.kafka.common.protocol.ApiKeys.LIST_GROUPS, LIST_GROUPS.latestVersion(), new ListGroupsResponseData()));

            // When
            client.getSync(new Request(LIST_TRANSACTIONS, LIST_TRANSACTIONS.latestVersion(), "client", new ListTransactionsRequestData()));

            // Then
            var requestAtBroker = tester.getOnlyRequestForApiKey(LIST_TRANSACTIONS).message();
            assertThat(unknownTaggedFieldsToStrings(requestAtBroker, FILTER_NAME_TAG))
                    .as("filter must have marked the request, proving sendRequest() reached the mock backend")
                    .containsExactly(RequestResponseMarkingFilter.class.getSimpleName() + "-" + filterName + "-request");
        }
    }

    // ---------------------------------------------------------------------------
    // C4: Concurrent out-of-band requests from two routes are each delivered back
    // ---------------------------------------------------------------------------

    /**
     * When one client request is fanned to two routes and each route's filter issues an out-of-band
     * {@code sendRequest()}, both out-of-band responses must be delivered back to the filter that
     * issued them. All out-of-band requests share one reserved correlation id, so the per-connection
     * {@code correlationId -> routeName} map collides; before the fix the replies were tagged with
     * the wrong route and written to the client, and both filters timed out after 20s.
     */
    @Test
    void concurrentOutOfBandRequestsFromTwoRoutesAreEachDelivered() {
        var markerA = "c4-marker-a";
        var markerB = "c4-marker-b";

        // Given
        var markerADef = new NamedFilterDefinitionBuilder(markerA, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(LIST_TRANSACTIONS),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.REQUEST),
                        "name", markerA,
                        "forwardingStyle", ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER)
                .build();
        var markerBDef = new NamedFilterDefinitionBuilder(markerB, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(LIST_TRANSACTIONS),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.REQUEST),
                        "name", markerB,
                        "forwardingStyle", ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER)
                .build();

        try (var tester = KroxyliciousTesters.mockKafkaKroxyliciousTester(
                mockBootstrap -> fanOutTwoRouteConfig(mockBootstrap, markerADef, markerBDef), ROUTING_ENABLED);
                var client = tester.simpleTestClient()) {

            ApiVersionsResponseData apiVersions = new ApiVersionsResponseData();
            apiVersions.apiKeys().add(new ApiVersionsResponseData.ApiVersion()
                    .setApiKey(FETCH.id).setMaxVersion(FETCH.latestVersion()).setMinVersion(FETCH.oldestVersion()));
            tester.addMockResponseForApiKey(new ResponsePayload(API_VERSIONS, API_VERSIONS.latestVersion(), apiVersions));
            tester.addMockResponseForApiKey(new ResponsePayload(LIST_TRANSACTIONS, LIST_TRANSACTIONS.latestVersion(), new ListTransactionsResponseData()));
            tester.addMockResponseForApiKey(
                    new ResponsePayload(org.apache.kafka.common.protocol.ApiKeys.LIST_GROUPS, LIST_GROUPS.latestVersion(), new ListGroupsResponseData()));

            // When
            var response = client.getSync(new Request(LIST_TRANSACTIONS, LIST_TRANSACTIONS.latestVersion(), "client", new ListTransactionsRequestData()));

            // Then
            assertThat(response)
                    .as("client request completes only if both routes' out-of-band responses were delivered")
                    .isNotNull();
            assertThat(tester.getRequestsForApiKey(org.apache.kafka.common.protocol.ApiKeys.LIST_GROUPS))
                    .as("both route filters must have issued their out-of-band request on the one connection")
                    .hasSize(2);
        }
    }

    // ---------------------------------------------------------------------------
    // C9: Concurrent out-of-band requests get distinct correlation ids
    // ---------------------------------------------------------------------------

    /**
     * The proxy no longer reads an out-of-band request's correlation id for its own delivery
     * logic - matching is done structurally via {@code PathElement} - but the value is still
     * visible to filter code, and some filters legitimately key their own bookkeeping off it
     * (e.g. a map from correlation id to pending state). If two concurrent out-of-band requests on
     * one connection were ever assigned the same id, such a filter would silently corrupt its own
     * state the same way the original route-tag corruption bug did to the proxy.
     */
    @Test
    void concurrentOutOfBandRequestsGetDistinctCorrelationIds() {
        var collectorId = "c9-" + System.identityHashCode(this);
        CorrelationIdCollectingFilter.reset(collectorId);

        // Given
        var collectorADef = new NamedFilterDefinitionBuilder("c9-collector-a", CorrelationIdCollectingFilterFactory.class.getName())
                .withConfig("collectorId", collectorId, "keyToTrigger", LIST_TRANSACTIONS)
                .build();
        var collectorBDef = new NamedFilterDefinitionBuilder("c9-collector-b", CorrelationIdCollectingFilterFactory.class.getName())
                .withConfig("collectorId", collectorId, "keyToTrigger", LIST_TRANSACTIONS)
                .build();

        try (var tester = KroxyliciousTesters.mockKafkaKroxyliciousTester(
                mockBootstrap -> fanOutTwoRouteConfig(mockBootstrap, collectorADef, collectorBDef), ROUTING_ENABLED);
                var client = tester.simpleTestClient()) {

            ApiVersionsResponseData apiVersions = new ApiVersionsResponseData();
            apiVersions.apiKeys().add(new ApiVersionsResponseData.ApiVersion()
                    .setApiKey(FETCH.id).setMaxVersion(FETCH.latestVersion()).setMinVersion(FETCH.oldestVersion()));
            tester.addMockResponseForApiKey(new ResponsePayload(API_VERSIONS, API_VERSIONS.latestVersion(), apiVersions));
            tester.addMockResponseForApiKey(new ResponsePayload(LIST_TRANSACTIONS, LIST_TRANSACTIONS.latestVersion(), new ListTransactionsResponseData()));
            tester.addMockResponseForApiKey(
                    new ResponsePayload(org.apache.kafka.common.protocol.ApiKeys.LIST_GROUPS, LIST_GROUPS.latestVersion(), new ListGroupsResponseData()));

            // When
            client.getSync(new Request(LIST_TRANSACTIONS, LIST_TRANSACTIONS.latestVersion(), "client", new ListTransactionsRequestData()));

            // Then
            assertThat(CorrelationIdCollectingFilter.observedFor(collectorId))
                    .as("two concurrent out-of-band requests on one connection must be assigned distinct "
                            + "correlation ids, since filter code may key its own bookkeeping off this value")
                    .hasSize(2);
        }
    }

    // ---------------------------------------------------------------------------
    // C10: Outer-route filter observes a response completed via a nested router's own dynamic dispatch
    // ---------------------------------------------------------------------------

    /**
     * Sibling observation (C8) matches a route's own position against a response's routing value via
     * {@code PathElement.RoutePosition#isAncestorOfOrSameAs}, which walks the response's whole route
     * position rather than comparing one hop. An outer route that targets a nested router is, structurally, an ancestor of
     * every route inside that nested router. {@code NestedRouteWithFiltersIT} already proves this for
     * requests (a filter on the outer route sees traffic forwarded into the nested router). This test
     * proves the same holds for responses: when the nested router dynamically dispatches a request via
     * {@code RouterContext.sendRequest()} - completed via
     * {@code RoutingHandler.handleNestedOobCompletion}, which writes the response back through the
     * pipeline rather than resolving it locally - the outer route's own filter must also observe that
     * response via {@code onResponse}, not just the nested route's own filter.
     */
    @Test
    void outerRouteFilterObservesResponseFromNestedRouterDynamicDispatch(KafkaCluster cluster, Topic topic) throws Exception {
        String outerCounterId = "c10-outer-" + topic.name();
        String innerCounterId = "c10-inner-" + topic.name();
        ResponseCountingFilter.reset(outerCounterId);
        ResponseCountingFilter.reset(innerCounterId);

        // Given
        var outerCounterDef = new NamedFilterDefinitionBuilder("c10-outer-counter", ResponseCountingFilterFactory.class.getName())
                .withConfig("counterId", outerCounterId)
                .build();
        var innerCounterDef = new NamedFilterDefinitionBuilder("c10-inner-counter", ResponseCountingFilterFactory.class.getName())
                .withConfig("counterId", innerCounterId)
                .build();
        var config = nestedRouteWithResponseCounters(cluster.getBootstrapServers(), outerCounterDef, innerCounterDef);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000))) {

            // When
            producer.send(new ProducerRecord<>(topic.name(), "key", "value")).get();
        }

        // Then
        assertThat(ResponseCountingFilter.countFor(innerCounterId, ApiKeys.PRODUCE))
                .as("the nested router's own route filter should observe its own dynamically-dispatched PRODUCE response")
                .isEqualTo(1);
        assertThat(ResponseCountingFilter.countFor(outerCounterId, ApiKeys.PRODUCE))
                .as("the outer route's filter should also observe that same response - the outer route is a "
                        + "structural ancestor of the nested route, and ancestor-path matching applies to responses "
                        + "completed via handleNestedOobCompletion the same way it applies to ordinary traffic")
                .isEqualTo(1);
    }

    // ---------------------------------------------------------------------------
    // C8: Sibling onResponse observation is not corrupted by concurrent OOB requests
    // ---------------------------------------------------------------------------

    /**
     * Per C6, a route's own (non-recipient) filters must observe every response - including
     * out-of-band ones - that flows through their route via {@code onResponse}, and must not observe
     * another route's traffic. All out-of-band requests share one reserved correlation id, so
     * {@code RoutingTerminalHandler} matches a returning response back to a route name via a
     * per-connection {@code correlationId -> routeName} map; with two routes' out-of-band requests
     * concurrently in flight that map collides, so a response can be restored with the wrong route
     * name (or none at all).
     * <p>
     * Delivery to the filter that actually issued the request is unaffected by this - it is matched
     * by recipient identity, not by route name - but sibling observation is not: it still keys off
     * the (possibly corrupted) route name carried on the frame. So a sibling filter can silently miss
     * its own route's out-of-band response, and/or spuriously observe another route's.
     */
    @Test
    void siblingFilterObservationIsNotCorruptedByConcurrentOutOfBandRequests() {
        var markerA = "c8-marker-a";
        var markerB = "c8-marker-b";
        var counterIdA = "c8-counter-a";
        var counterIdB = "c8-counter-b";
        ResponseCountingFilter.reset(counterIdA);
        ResponseCountingFilter.reset(counterIdB);

        // Given
        var markerADef = new NamedFilterDefinitionBuilder(markerA, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(LIST_TRANSACTIONS),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.REQUEST),
                        "name", markerA,
                        "forwardingStyle", ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER)
                .build();
        var markerBDef = new NamedFilterDefinitionBuilder(markerB, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(LIST_TRANSACTIONS),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.REQUEST),
                        "name", markerB,
                        "forwardingStyle", ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER)
                .build();
        var counterADef = new NamedFilterDefinitionBuilder("c8-counter-def-a", ResponseCountingFilterFactory.class.getName())
                .withConfig("counterId", counterIdA)
                .build();
        var counterBDef = new NamedFilterDefinitionBuilder("c8-counter-def-b", ResponseCountingFilterFactory.class.getName())
                .withConfig("counterId", counterIdB)
                .build();

        try (var tester = KroxyliciousTesters.mockKafkaKroxyliciousTester(
                mockBootstrap -> fanOutTwoRouteConfigWithSiblingCounters(mockBootstrap, markerADef, counterADef, markerBDef, counterBDef), ROUTING_ENABLED);
                var client = tester.simpleTestClient()) {

            ApiVersionsResponseData apiVersions = new ApiVersionsResponseData();
            apiVersions.apiKeys().add(new ApiVersionsResponseData.ApiVersion()
                    .setApiKey(FETCH.id).setMaxVersion(FETCH.latestVersion()).setMinVersion(FETCH.oldestVersion()));
            tester.addMockResponseForApiKey(new ResponsePayload(API_VERSIONS, API_VERSIONS.latestVersion(), apiVersions));
            tester.addMockResponseForApiKey(new ResponsePayload(LIST_TRANSACTIONS, LIST_TRANSACTIONS.latestVersion(), new ListTransactionsResponseData()));
            tester.addMockResponseForApiKey(
                    new ResponsePayload(org.apache.kafka.common.protocol.ApiKeys.LIST_GROUPS, LIST_GROUPS.latestVersion(), new ListGroupsResponseData()));

            // When
            client.getSync(new Request(LIST_TRANSACTIONS, LIST_TRANSACTIONS.latestVersion(), "client", new ListTransactionsRequestData()));

            // Then
            long observedByA = ResponseCountingFilter.countFor(counterIdA, LIST_GROUPS);
            long observedByB = ResponseCountingFilter.countFor(counterIdB, LIST_GROUPS);
            assertThat(observedByA + observedByB)
                    .as("each route's own out-of-band response should be observed, via onResponse, by that route's own "
                            + "sibling filter exactly once (C6), so the total across both routes should be 2 "
                            + "(route-a observed %d, route-b observed %d). A lower total means the shared out-of-band "
                            + "correlation id corrupted a response's route tag badly enough that no route's filters observed it",
                            observedByA, observedByB)
                    .isEqualTo(2);
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
        assertThat(RequestCountingFilter.countFor(counterId, io.kroxylicious.kafka.common.protocol.ApiKeys.LIST_GROUPS))
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
        assertThat(RequestCountingFilter.countFor(counterIdA, io.kroxylicious.kafka.common.protocol.ApiKeys.LIST_GROUPS))
                .as("route-a's counting filter should see the internal LIST_GROUPS from the marking filter on the same route")
                .isPositive();
        assertThat(RequestCountingFilter.countFor(counterIdB, io.kroxylicious.kafka.common.protocol.ApiKeys.LIST_GROUPS))
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
            client.getSync(new Request(org.apache.kafka.common.protocol.ApiKeys.METADATA, (short) 12, "metadata-client", new MetadataRequestData()));

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
        var records = KafkaRecordTestUtils.singleElementMemoryRecords("key", "value");
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
        return new Request(org.apache.kafka.common.protocol.ApiKeys.PRODUCE, (short) 9, "test-client", produceData);

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

    private ConfigurationBuilder fanOutTwoRouteConfig(String bootstrapServers,
                                                      NamedFilterDefinition routeAFilter,
                                                      NamedFilterDefinition routeBFilter) {
        var clusterDef = new ClusterDefinition(CLUSTER_NAME, bootstrapServers, null);
        var routeA = new RouteDefinition(ROUTE_A, 0, List.of(routeAFilter.name()), new RouteTarget(CLUSTER_NAME, null));
        var routeB = new RouteDefinition(ROUTE_B, 1, List.of(routeBFilter.name()), new RouteTarget(CLUSTER_NAME, null));
        var routerDef = new RouterDefinition(ROUTER_NAME, FanOutRouterFactory.class.getName(),
                new FanOutRouterFactory.Config(List.of(ROUTE_A, ROUTE_B), LIST_TRANSACTIONS.name()), List.of(routeA, routeB));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();
        return baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToFilterDefinitions(routeAFilter)
                .addToFilterDefinitions(routeBFilter)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }

    private ConfigurationBuilder fanOutTwoRouteConfigWithSiblingCounters(String bootstrapServers,
                                                                         NamedFilterDefinition routeAMarker,
                                                                         NamedFilterDefinition routeACounter,
                                                                         NamedFilterDefinition routeBMarker,
                                                                         NamedFilterDefinition routeBCounter) {
        var clusterDef = new ClusterDefinition(CLUSTER_NAME, bootstrapServers, null);
        var routeA = new RouteDefinition(ROUTE_A, 0, List.of(routeAMarker.name(), routeACounter.name()), new RouteTarget(CLUSTER_NAME, null));
        var routeB = new RouteDefinition(ROUTE_B, 1, List.of(routeBMarker.name(), routeBCounter.name()), new RouteTarget(CLUSTER_NAME, null));
        var routerDef = new RouterDefinition(ROUTER_NAME, FanOutRouterFactory.class.getName(),
                new FanOutRouterFactory.Config(List.of(ROUTE_A, ROUTE_B), LIST_TRANSACTIONS.name()), List.of(routeA, routeB));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();
        return baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToFilterDefinitions(routeAMarker)
                .addToFilterDefinitions(routeACounter)
                .addToFilterDefinitions(routeBMarker)
                .addToFilterDefinitions(routeBCounter)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }

    private ConfigurationBuilder nestedRouteWithResponseCounters(String bootstrapServers,
                                                                 NamedFilterDefinition outerCounterFilter,
                                                                 NamedFilterDefinition innerCounterFilter) {
        var clusterDef = new ClusterDefinition(CLUSTER_NAME, bootstrapServers, null);

        var innerRoute = new RouteDefinition("backend", 0, List.of(innerCounterFilter.name()), new RouteTarget(CLUSTER_NAME, null));
        var innerRouterDef = new RouterDefinition("inner", DynamicProduceRouterFactory.class.getName(),
                new DynamicProduceRouterFactory.Config("backend"), List.of(innerRoute));

        var outerRoute = new RouteDefinition("nested", 0, List.of(outerCounterFilter.name()), new RouteTarget(null, "inner"));
        var outerRouterDef = new RouterDefinition(ROUTER_NAME, PassThroughRouterFactory.class.getName(),
                new PassThroughRouterFactory.Config("nested"), List.of(outerRoute));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();
        return baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToFilterDefinitions(outerCounterFilter)
                .addToFilterDefinitions(innerCounterFilter)
                .addToRouterDefinitions(outerRouterDef, innerRouterDef)
                .addToVirtualClusters(vc);
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
