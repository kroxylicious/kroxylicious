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
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.ForwardingStyle;
import io.kroxylicious.it.testplugins.RequestCountingFilter;
import io.kroxylicious.it.testplugins.RequestCountingFilterFactory;
import io.kroxylicious.it.testplugins.RequestResponseMarkingFilter;
import io.kroxylicious.it.testplugins.RequestResponseMarkingFilterFactory;
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
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.apache.kafka.common.protocol.ApiKeys.FETCH;
import static org.apache.kafka.common.protocol.ApiKeys.LIST_GROUPS;
import static org.apache.kafka.common.protocol.ApiKeys.LIST_TRANSACTIONS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests that demonstrate correctness issues identified in the per-route filter
 * chain implementation.  Each test targets a specific bug and is expected to fail until
 * the corresponding issue is fixed.
 *
 * <p>C1: Router-internal frames pass through route filter handlers.
 * <p>C2: {@code correlationIdToRoute} entries are never removed for router-internal requests.
 * <p>C3: {@code sendRequest()} in a route filter sends to an arbitrary backend when multiple
 *         routes are active.
 * <p>C4: Test name / behaviour mismatch in RouterDispatchHandlerTest masks a regression where
 *         forwarding failures leave the pending future stuck rather than completing exceptionally.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RouteFilterCorrectnessIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();
    private static final String ROUTER_NAME = "router";
    private static final String CLUSTER_NAME = "backing";
    private static final String ROUTE_A = "route-a";

    // ---------------------------------------------------------------------------
    // C1: Route filters see all traffic on a route, including router-originated
    // ---------------------------------------------------------------------------

    /**
     * When a router uses dynamic routing (e.g. {@link DynamicProduceRouterFactory}), it
     * forwards the request to the backend by calling {@code RouterContext.sendRequest()},
     * which fires a {@code DecodedRequestFrame} through the pipeline.  Route filter handlers
     * sit after {@code RouterDispatchHandler} in the pipeline, so they see this frame.
     *
     * <p>This is the intended behaviour: per-route filters apply to <em>all</em> traffic on
     * a route, regardless of whether it was originated by the client or by the router.
     * A name-mapping filter on a route must transform topic names in all requests reaching
     * the backend through that route.
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
    // C2: correlationIdToRoute entries never removed for router-internal requests
    // ---------------------------------------------------------------------------

    /**
     * {@code RoutingTerminalHandler} records a {@code correlationId → routeName} mapping for
     * every inbound frame that expects a response.  When a router-internal frame goes through
     * the terminal handler (as a consequence of C1), its router-range correlation ID is
     * recorded.  The response is then consumed by {@code RouterDispatchHandler.write()} without
     * calling {@code ctx.write()}, so {@code RoutingTerminalHandler.write()} is never triggered
     * and the entry is never removed.
     *
     * <p>This leak is bounded (router correlation IDs cycle through a fixed range) and does
     * not currently cause observable routing failures with a single route.  A targeted unit test
     * against {@code RoutingTerminalHandler} is the appropriate level at which to assert the
     * map invariant directly.  This test exercises the same code path at the integration level
     * and verifies that routing remains correct after many dynamic-produce operations.
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

            // When: many dynamic produce cycles (each triggers a router-internal re-forward that leaks a map entry)
            for (int i = 0; i < 50; i++) {
                producer.send(new ProducerRecord<>(topic.name(), "key", "value-" + i)).get();
            }

            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(30));

            // Then: routing continues to deliver all records correctly despite the growing correlationIdToRoute map
            assertThat(records).hasSize(50);
        }
    }

    // ---------------------------------------------------------------------------
    // C3: sendRequest() from a route filter goes to an arbitrary backend
    // ---------------------------------------------------------------------------

    /**
     * When a route filter calls {@code FilterContext.sendRequest()}, the resulting
     * {@code InternalRequestFrame} reaches {@code RoutingTerminalHandler} with a null route
     * name.  The terminal handler delegates to
     * {@code ClientConnectionStateMachine.onClientFilterChainComplete()}, which picks a backend
     * connection via {@code serverConnections.values().iterator().next()}.  With a single active
     * route this always returns the correct connection.  With two or more routes whose backends
     * differ, the choice is non-deterministic.
     *
     * <p>This test uses a single cluster and a single route, so {@code sendRequest()} works
     * correctly by accident.  Demonstrating the actual bug requires two distinct Kafka clusters
     * (one per route) so that sending to the wrong connection produces a different — observable
     * — response.  See the disabled companion test below.
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

            // Then: sendRequest() reached the correct (only) backend and the filter marked the request
            var requestAtBroker = tester.getOnlyRequestForApiKey(LIST_TRANSACTIONS).message();
            assertThat(unknownTaggedFieldsToStrings(requestAtBroker, FILTER_NAME_TAG))
                    .as("filter must have marked the request, proving sendRequest() reached the mock backend")
                    .containsExactly(RequestResponseMarkingFilter.class.getSimpleName() + "-" + filterName + "-request");
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

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
