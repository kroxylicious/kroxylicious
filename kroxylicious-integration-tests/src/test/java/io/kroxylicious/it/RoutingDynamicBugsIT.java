/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.ForwardingStyle;
import io.kroxylicious.it.testplugins.RequestResponseMarkingFilter;
import io.kroxylicious.it.testplugins.RequestResponseMarkingFilterFactory;
import io.kroxylicious.it.testplugins.router.ContextCapturingRouterFactory;
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
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.it.UnknownTaggedFields.unknownTaggedFieldsToStrings;
import static io.kroxylicious.it.testplugins.RequestResponseMarkingFilter.FILTER_NAME_TAG;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultGatewayBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces three bugs that exist in main and are fixed in commit 057a406d3
 * (branch {@code prototype-that-routing-change}):
 *
 * <p>Bug 1: {@link #filterSendRequestHangsWhenApiKeyDynamicallyRouted}
 * — A VC-level filter's {@code FilterContext.sendRequest()} OOB fires from
 * FilterHandler toward RouterDispatchHandler, which runs {@code dispatchDynamically}.
 * On main (empty {@code staticRoutes()}), the router's {@code RespondWith} is
 * delivered back as a {@code DecodedResponseFrame}, which FilterHandler dispatches
 * to {@code onResponse} instead of completing the OOB promise. The filter's
 * {@code onRequest} hangs and the PRODUCE never reaches the broker.
 *
 * <p>Bug 2: {@link #routeFilterOnResponseNotCalledForDynamicResponse}
 * — A route filter's {@code onResponse} is never invoked for dynamically-dispatched
 * responses. On main, {@code RoutingTerminalHandler} skips routing-correlationId frames
 * ({@code isRouterInternal=true}) when populating {@code correlationIdToRoute}. The
 * routing response arrives at {@code RouteFilterHandler} without a route name;
 * {@code matchesRoute()} returns false and the filter is bypassed.
 *
 * <p>Bug 3: {@link #routeFilterOobFromOnResponseDeadlocks}
 * — On main, Bug 2 prevents {@code onResponse} from being called, so this case does not
 * manifest independently. Once Bug 2 is fixed, the OOB response queued in
 * {@code FilterHandler.writeFuture} cannot complete until the routing response
 * {@code writeFuture} resolves; but that future waits for the OOB promise, which is
 * queued after it. Circular wait: the response never arrives.
 *
 * <p>Bug 4: {@link #eagerMetadataLearnerOobHangsWhenApiKeyDynamicallyRouted}
 * — Same root cause as Bug 1 but via an internal filter. When a client connects
 * directly to a per-node port before the proxy has learned the upstream topology,
 * {@code EagerMetadataLearner} is installed and fires an OOB METADATA request.
 * On main this OOB enters {@code dispatchDynamically} and its promise never resolves,
 * so the connection never closes and the client cannot reconnect.
 *
 * <p>All four tests use {@link ContextCapturingRouterFactory}, which on main routes
 * all API keys dynamically (empty {@code staticRoutes()}). On the fixed commit the same
 * factory overrides {@code shouldIntercept()} to return {@code true} unconditionally,
 * keeping all traffic on the dynamic path so the fixes are exercised.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RoutingDynamicBugsIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();
    private static final String ROUTE_NAME = "default-route";
    private static final String ROUTER_NAME = "capturing-router";
    private static final String CLUSTER_NAME = "backing";

    @BeforeEach
    void resetRouterState() {
        ContextCapturingRouterFactory.reset();
    }

    // -----------------------------------------------------------------------
    // Bug 1: FilterContext.sendRequest() hangs when API key is dynamically routed
    // -----------------------------------------------------------------------

    /**
     * A VC-level (virtual-cluster) filter calls {@code FilterContext.sendRequest()} (OOB
     * LIST_GROUPS) inside {@code onRequest} for PRODUCE. VC-level filters sit before
     * {@code RouterDispatchHandler} in the pipeline, so the OOB {@code InternalRequestFrame}
     * reaches {@code channelRead} and enters {@code dispatchDynamically}.
     *
     * <p>On main, LIST_GROUPS is also dynamically routed (empty {@code staticRoutes()}).
     * The router handles the OOB, responds with {@code RespondWith}, but
     * {@code deliverResponse} creates a {@code DecodedResponseFrame}, which
     * {@code FilterHandler.write()} dispatches to {@code onResponse} instead of
     * completing the filter promise. The filter's {@code onRequest} for PRODUCE is
     * permanently blocked, so PRODUCE never reaches the broker and the client times out.
     *
     * <p>Fixed in 057a406d3: {@code dispatchDynamically} detects {@code InternalRequestFrame},
     * skips its sequence slot immediately, and delivers the response via
     * {@code channel.writeAndFlush(InternalResponseFrame)}, which {@code FilterHandler}
     * recognises as an OOB response and uses to complete the filter promise.
     */
    @Test
    void filterSendRequestHangsWhenApiKeyDynamicallyRouted(KafkaCluster cluster, Topic topic) {
        var filterName = "bug1-filter";

        // Given: VC-level filter that sends an OOB from onRequest for PRODUCE
        var filterDef = new NamedFilterDefinitionBuilder(filterName, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(ApiKeys.PRODUCE),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.REQUEST),
                        "name", filterName,
                        "forwardingStyle", ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER)
                .build();
        var config = dynamicRouterConfigWithVcFilter(cluster.getBootstrapServers(), filterDef);

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

    // -----------------------------------------------------------------------
    // Bug 2: Route filter onResponse not called for dynamically-dispatched response
    // -----------------------------------------------------------------------

    /**
     * A route filter's {@code onResponse} for PRODUCE is never invoked when the router
     * uses dynamic dispatch (all keys in the dynamic path on main). On main,
     * {@code RoutingTerminalHandler.channelRead} guards the {@code correlationIdToRoute}
     * insertion with {@code !isRouterInternal}, so routing-range correlation IDs are never
     * recorded. The routing PRODUCE response arrives at {@code RouteFilterHandler.write()}
     * without a route name, {@code matchesRoute()} returns false, and the filter's tag
     * is absent from the response delivered to the client.
     *
     * <p>Fixed in 057a406d3: the {@code isRouterInternal} guard is removed so all frames
     * (including router-internal routing requests) populate {@code correlationIdToRoute}.
     * Routing responses then carry the route name when they reach {@code RouteFilterHandler},
     * which calls {@code onResponse} and makes filter modifications visible in the response
     * body used to construct the final client-facing frame.
     */
    @Test
    void routeFilterOnResponseNotCalledForDynamicResponse(KafkaCluster cluster, Topic topic) {
        var filterName = "bug2-filter";

        // Given: route-level filter that marks the PRODUCE response body
        var filterDef = new NamedFilterDefinitionBuilder(filterName, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(ApiKeys.PRODUCE),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.RESPONSE),
                        "name", filterName,
                        "forwardingStyle", ForwardingStyle.SYNCHRONOUS)
                .build();
        var config = dynamicRouterConfigWithRouteFilter(cluster.getBootstrapServers(), filterDef);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var client = tester.simpleTestClient()) {

            // Given: establish the upstream connection
            client.getSync(new Request(ApiKeys.METADATA, (short) 12, "metadata-client", new MetadataRequestData()));

            // When
            var response = client.getSync(produceRequest(topic.name(), (short) 1));

            // Then: the filter's tag must be on the response body, proving onResponse was called
            assertThat(unknownTaggedFieldsToStrings(response.payload().message(), FILTER_NAME_TAG))
                    .as("route filter onResponse must be invoked for dynamically-dispatched PRODUCE responses")
                    .containsExactly(RequestResponseMarkingFilter.class.getSimpleName() + "-" + filterName + "-response");
        }
    }

    // -----------------------------------------------------------------------
    // Bug 3: Deadlock when route filter sends OOB from onResponse of dynamic response
    // -----------------------------------------------------------------------

    /**
     * A route filter sends an OOB (LIST_GROUPS via {@code FilterContext.sendRequest()})
     * inside {@code onResponse} for a dynamically-dispatched PRODUCE.
     *
     * <p>On main, Bug 2 prevents {@code onResponse} from being called, so the test fails
     * because the response arrives without the filter's tag (no deadlock — just the wrong
     * assertion result).
     *
     * <p>Once Bug 2 is fixed but before Bug 3 is fixed (057a406d3 intermediate), the OOB
     * LIST_GROUPS response is queued in {@code FilterHandler.writeFuture} behind the
     * in-progress routing PRODUCE response. The routing response cannot complete until
     * {@code onResponse} returns; {@code onResponse} waits for the OOB promise; the OOB
     * promise completion is queued after the routing response. The response never arrives
     * and the assertion times out.
     *
     * <p>Fixed in 057a406d3: {@code FilterHandler.handleInternalResponseWrite} completes
     * the filter promise immediately when the frame's recipient matches the current filter,
     * bypassing {@code writeFuture} and breaking the cycle.
     */
    @Test
    void routeFilterOobFromOnResponseDeadlocks(KafkaCluster cluster, Topic topic) {
        var filterName = "bug3-filter";

        // Given: route-level filter that sends an OOB from onResponse for PRODUCE
        var filterDef = new NamedFilterDefinitionBuilder(filterName, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(ApiKeys.PRODUCE),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.RESPONSE),
                        "name", filterName,
                        "forwardingStyle", ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER)
                .build();
        var config = dynamicRouterConfigWithRouteFilter(cluster.getBootstrapServers(), filterDef);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var client = tester.simpleTestClient()) {

            // Given: establish the upstream connection
            client.getSync(new Request(ApiKeys.METADATA, (short) 12, "metadata-client", new MetadataRequestData()));

            // When: async so the test fails fast if the response deadlocks
            var futureResponse = client.get(produceRequest(topic.name(), (short) 1));

            // Then: response must arrive without deadlock and carry the filter's tag
            assertThat(futureResponse)
                    .as("PRODUCE response must arrive: OOB from route filter onResponse must not deadlock")
                    .succeedsWithin(Duration.ofSeconds(5));
            var response = futureResponse.toCompletableFuture().join();
            assertThat(unknownTaggedFieldsToStrings(response.payload().message(), FILTER_NAME_TAG))
                    .as("route filter onResponse must be called and its OOB must complete before the response is forwarded")
                    .containsExactly(RequestResponseMarkingFilter.class.getSimpleName() + "-" + filterName + "-response");
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Bug 4: EagerMetadataLearner OOB hangs when API key is dynamically routed
    // -----------------------------------------------------------------------

    /**
     * When a client connects directly to a per-node port before the proxy has learned
     * the upstream topology, {@code EagerMetadataLearner} is installed as an internal
     * VC-level filter. Like the user-facing filter in
     * {@link #filterSendRequestHangsWhenApiKeyDynamicallyRouted}, it fires an OOB METADATA
     * request via {@code FilterContext.sendRequest()}.
     *
     * <p>On main, the OOB {@code InternalRequestFrame} reaches {@code RouterDispatchHandler}
     * and enters {@code dispatchDynamically} (METADATA is not in the empty
     * {@code staticRoutes()}). The router responds with {@code RespondWith}, but
     * {@code deliverResponse} creates a {@code DecodedResponseFrame}, which
     * {@code FilterHandler.write()} dispatches to {@code onResponse} instead of
     * completing the filter promise. {@code EagerMetadataLearner} never receives the
     * METADATA response, the connection never closes, and the client cannot reconnect.
     *
     * <p>The expected behaviour on the fixed commit is:
     * <ol>
     *   <li>First connection to the per-node port: {@code EagerMetadataLearner} fires OOB
     *       METADATA, receives the response, returns it to the client, and closes the
     *       connection so the client reconnects to the correct broker.</li>
     *   <li>Second connection: upstream topology is now known, no {@code EagerMetadataLearner},
     *       and subsequent requests (e.g. PRODUCE) are routed normally.</li>
     * </ol>
     */
    @Test
    void eagerMetadataLearnerOobHangsWhenApiKeyDynamicallyRouted(KafkaCluster cluster, Topic topic) {
        // Given
        var config = dynamicRouterConfigWithNodePort(cluster.getBootstrapServers());

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
     * Config with the filter at VC level (before RouterDispatchHandler in the pipeline).
     * OOBs fired by the filter reach RouterDispatchHandler and enter dispatchDynamically.
     */
    private ConfigurationBuilder dynamicRouterConfigWithVcFilter(String bootstrapServers, NamedFilterDefinition filterDef) {
        var clusterDef = new ClusterDefinition(CLUSTER_NAME, bootstrapServers, null);
        var route = new RouteDefinition(ROUTE_NAME, 0, List.of(), new RouteTarget(CLUSTER_NAME, null));
        var routerConfig = new ContextCapturingRouterFactory.Config(ROUTE_NAME);
        var routerDef = new RouterDefinition(ROUTER_NAME, ContextCapturingRouterFactory.class.getName(), routerConfig, List.of(route));
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

    /**
     * Config with the filter at route level (after RouterDispatchHandler in the pipeline).
     * The filter's onResponse is invoked (or not, depending on the bug) for routing
     * responses flowing back through the route.
     */
    private ConfigurationBuilder dynamicRouterConfigWithRouteFilter(String bootstrapServers, NamedFilterDefinition filterDef) {
        var clusterDef = new ClusterDefinition(CLUSTER_NAME, bootstrapServers, null);
        var route = new RouteDefinition(ROUTE_NAME, 0, List.of(filterDef.name()), new RouteTarget(CLUSTER_NAME, null));
        var routerConfig = new ContextCapturingRouterFactory.Config(ROUTE_NAME);
        var routerDef = new RouterDefinition(ROUTER_NAME, ContextCapturingRouterFactory.class.getName(), routerConfig, List.of(route));
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

    /**
     * Config with a {@code portIdentifiesNode} gateway that exposes port 9192 as bootstrap
     * and port 9193 as the per-node port for virtual node 0. Connecting to port 9193 before
     * the proxy has learned the upstream topology installs {@code EagerMetadataLearner}.
     */
    private ConfigurationBuilder dynamicRouterConfigWithNodePort(String bootstrapServers) {
        var clusterDef = new ClusterDefinition(CLUSTER_NAME, bootstrapServers, null);
        var route = new RouteDefinition(ROUTE_NAME, 0, List.of(), new RouteTarget(CLUSTER_NAME, null));
        var routerConfig = new ContextCapturingRouterFactory.Config(ROUTE_NAME);
        var routerDef = new RouterDefinition(ROUTER_NAME, ContextCapturingRouterFactory.class.getName(), routerConfig, List.of(route));
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
}
