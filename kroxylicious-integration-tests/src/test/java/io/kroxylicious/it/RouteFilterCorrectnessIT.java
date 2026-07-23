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
import org.junit.jupiter.api.Disabled;
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
import io.kroxylicious.it.testplugins.router.SplitStaticRouterFactory;
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
import io.kroxylicious.testing.integration.Response;
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
    // C1: Router-internal frames pass through route filter handlers
    // ---------------------------------------------------------------------------

    /**
     * When a router uses dynamic routing (e.g. {@link DynamicProduceRouterFactory}), it
     * forwards the original request to the backend by calling
     * {@code RouterContext.sendRequest()}, which creates a plain {@code DecodedRequestFrame}
     * and fires it back through the inbound pipeline via {@code ctx.fireChannelRead()}.
     * Because route filter handlers sit after {@code RouterDispatchHandler} in the pipeline,
     * these router-internal frames pass through them.  A route filter that intercepts PRODUCE
     * will therefore be invoked once for the client's request <em>and</em> once for the
     * router's re-forwarded copy — a count of 2 instead of the expected 1.
     *
     * <p>This test fails until C1 is fixed.
     */
    @Test
    void routeFiltersAreErroneouslyInvokedOnRouterInternalFrames(KafkaCluster cluster, Topic topic) throws Exception {
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

        // Then: the route filter should have been invoked exactly once, for the client's PRODUCE.
        // C1 bug: RouterDispatchHandler also fires the re-forwarded PRODUCE through the pipeline,
        // so the count is 2.
        assertThat(RequestCountingFilter.countFor(counterId, ApiKeys.PRODUCE))
                .as("route filter should see only client-originated PRODUCE requests, not router-internal copies")
                .isEqualTo(1);
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
            Response response = client.getSync(new Request(LIST_TRANSACTIONS, LIST_TRANSACTIONS.latestVersion(), "client", new ListTransactionsRequestData()));

            // Then: sendRequest() reached the correct (only) backend and the filter marked the request
            var requestAtBroker = tester.getOnlyRequestForApiKey(LIST_TRANSACTIONS).message();
            assertThat(unknownTaggedFieldsToStrings(requestAtBroker, FILTER_NAME_TAG))
                    .as("filter must have marked the request, proving sendRequest() reached the mock backend")
                    .containsExactly(RequestResponseMarkingFilter.class.getSimpleName() + "-" + filterName + "-request");
        }
    }

    /**
     * Two-cluster companion to {@link #routeFilterSendRequestWorksWithSingleRoute}.
     * With two routes pointing to two distinct clusters, the non-deterministic
     * {@code iterator().next()} in {@code ClientConnectionStateMachine.onClientFilterChainComplete()}
     * will sometimes send the {@code sendRequest()} to the wrong cluster, producing a response
     * the filter did not expect (e.g. a topic absent on the wrong cluster, or a different cluster ID).
     *
     * <p>This test is disabled pending investigation of multi-cluster
     * {@code KafkaClusterExtension} support.  Once two distinct {@code KafkaCluster} instances
     * can be injected, the setup should use {@link SplitStaticRouterFactory} to route PRODUCE
     * to route-A (cluster1) and LIST_TRANSACTIONS to route-B (cluster2).  A route-A filter that
     * uses {@code sendRequest()} and expects cluster1-specific data will fail when the request
     * accidentally reaches cluster2.
     */
    @Test
    @Disabled("C3: requires two distinct Kafka clusters injected by KafkaClusterExtension")
    void routeFilterSendRequestGoesToCorrectBackendWithMultipleRoutes(KafkaCluster cluster1, KafkaCluster cluster2, Topic topicOnCluster1) {
        // Setup outline:
        // 1. SplitStaticRouterFactory: PRODUCE → route-A (cluster1), LIST_TRANSACTIONS → route-B (cluster2)
        // 2. Route-A has a filter that uses sendRequest() to fetch METADATA for topicOnCluster1
        // 3. Client sends PRODUCE (establishes route-A connection) then LIST_TRANSACTIONS (establishes route-B connection)
        // 4. Filter's sendRequest() should always reach cluster1 (topic found)
        // 5. C3 bug: sendRequest() sometimes reaches cluster2 (topic absent → assertion fails)
        throw new UnsupportedOperationException("not yet implemented");
    }

    // ---------------------------------------------------------------------------
    // C4: RouterDispatchHandlerTest name mismatch masks a regression
    // ---------------------------------------------------------------------------

    /**
     * {@code RouterDispatchHandlerTest.sendToAnyNodeShouldReturnFailedFutureWhenForwardThrows}
     * asserts {@code isNotDone()} — the future is indefinitely stuck — but its name claims the
     * future is failed.  Before this branch, the exception propagated through
     * {@code doSendToAny}'s catch block and returned {@code CompletableFuture.failedFuture(e)}.
     * After the branch, the exception travels through Netty's {@code exceptionCaught} pipeline
     * path, leaving the pending response entry and the future permanently pending.
     *
     * <p>The observable IT-level symptom is a client request that never receives a response.
     * The test below exercises the forwarding path with a route that will fail to connect,
     * and verifies that the client receives a timely error rather than hanging.
     *
     * <p>Reproducing the exact regression reliably requires injecting a fault into
     * {@code forwardToRoute}.  The unit test in {@code RouterDispatchHandlerTest} is the
     * appropriate vehicle for pinning the correct behaviour; the name of that test should be
     * updated to reflect the actual observed semantics before fixing the bug.
     */
    @Test
    @Disabled("C4: requires RouterDispatchHandlerTest to be fixed first to document expected behaviour")
    void routingFailureCompletesClientFutureExceptionallyRatherThanHanging(KafkaCluster cluster, Topic topic) throws Exception {
        // Setup outline:
        // 1. Configure a route that initially connects successfully
        // 2. After connection, sever the link to the backend (e.g. by stopping the cluster)
        // 3. Client sends a request whose forwarding via forwardToRoute throws
        // 4. Expected: client receives an error within a reasonable timeout (not a hung future)
        // 5. C4 bug: the future is never completed, the client hangs indefinitely
        throw new UnsupportedOperationException("not yet implemented");
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
