/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.util.List;
import java.util.Map;
import java.util.Set;

import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.message.CreateTopicsResponseData;
import io.kroxylicious.kafka.common.message.DescribeClusterRequestData;
import io.kroxylicious.kafka.common.message.DescribeClusterResponseData;
import io.kroxylicious.kafka.common.message.ListGroupsResponseData;
import io.kroxylicious.kafka.common.message.ListTransactionsRequestData;
import io.kroxylicious.kafka.common.message.ListTransactionsResponseData;
import io.kroxylicious.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.ForwardingStyle;
import io.kroxylicious.it.testplugins.OutOfBandSendFilterFactory;
import io.kroxylicious.it.testplugins.RequestResponseMarkingFilter;
import io.kroxylicious.it.testplugins.RequestResponseMarkingFilterFactory;
import io.kroxylicious.it.testplugins.router.DynamicProduceRouterFactory;
import io.kroxylicious.it.testplugins.router.PassThroughRouterFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
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

import static io.kroxylicious.it.UnknownTaggedFields.unknownTaggedFieldsToStrings;
import static io.kroxylicious.it.testplugins.RequestResponseMarkingFilter.FILTER_NAME_TAG;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.OS_ASSIGNED_BOOTSTRAP;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.CREATE_TOPICS;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.DESCRIBE_CLUSTER;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.FETCH;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.LIST_GROUPS;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.LIST_TRANSACTIONS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a filter on an outer route (targeting a nested router) can
 * send out-of-band (OOB) requests correctly.
 *
 * <p>The bug: the nested {@code RoutingHandler} was routing OOB
 * {@link io.kroxylicious.proxy.frame.InternalRequestFrame}s to
 * {@code handleCompletion} instead of {@code handleNestedOobCompletion},
 * so the OOB promise was never completed and the originating filter hung.
 *
 * <p>Topology:
 * <pre>
 * Client → demo VC
 *   → outer router (PassThrough → "to-inner")
 *   → outer route "to-inner" [OutOfBandSendFilter]
 *   → inner router (DynamicProduce → "backend")
 *   → inner route "backend" [RequestResponseMarkingFilter]
 *   → mock server
 * </pre>
 */
@ExtendWith(NettyLeakDetectorExtension.class)
class NestedRouterOobIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();

    @Test
    void outerRouteFilterShouldHandleOobRequestViaNestedRouter() {
        // Given
        var outerOobFilter = new NamedFilterDefinitionBuilder("oob-sender", OutOfBandSendFilterFactory.class.getName())
                .withConfig(Map.of("apiKeyToSend", CREATE_TOPICS, "tagToCollect", FILTER_NAME_TAG))
                .build();
        var innerMarkerFilter = new NamedFilterDefinitionBuilder("inner-marker", RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("name", "inner-marker", "keysToMark", Set.of(CREATE_TOPICS))
                .build();

        var innerRoute = new RouteDefinition("backend", 0, List.of("inner-marker"), new RouteTarget("mock-cluster", null));
        var innerRouter = new RouterDefinition("inner",
                DynamicProduceRouterFactory.class.getName(),
                new DynamicProduceRouterFactory.Config("backend"),
                List.of(innerRoute));

        var outerRoute = new RouteDefinition("to-inner", 0, List.of("oob-sender"), new RouteTarget(null, "inner"));
        var outerRouter = new RouterDefinition("outer",
                PassThroughRouterFactory.class.getName(),
                new PassThroughRouterFactory.Config("to-inner"),
                List.of(outerRoute));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "outer"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(OS_ASSIGNED_BOOTSTRAP).build())
                .build();

        try (var tester = KroxyliciousTesters.mockKafkaKroxyliciousTester(s -> {
            var clusterDef = new ClusterDefinition("mock-cluster", s, null);
            return baseConfigurationBuilder()
                    .addToClusterDefinitions(clusterDef)
                    .addToFilterDefinitions(outerOobFilter, innerMarkerFilter)
                    .addToRouterDefinitions(outerRouter, innerRouter)
                    .addToVirtualClusters(vc);
        }, ROUTING_ENABLED);
                var client = tester.simpleTestClient()) {

            tester.addMockResponseForApiKey(new ResponsePayload(CREATE_TOPICS, CREATE_TOPICS.latestVersion(), arbitraryCreateTopicsResponse()));
            tester.addMockResponseForApiKey(new ResponsePayload(DESCRIBE_CLUSTER, DESCRIBE_CLUSTER.latestVersion(), arbitraryDescribeClusterResponse()));

            // When
            var response = client.getSync(
                    new Request(DESCRIBE_CLUSTER, DESCRIBE_CLUSTER.latestVersion(), "client", new DescribeClusterRequestData()));

            // Then
            var responseData = (DescribeClusterResponseData) response.payload().message();
            assertThat(responseData.errorMessage()).isEqualTo(
                    "filterNameTaggedFieldsFromOutOfBandResponse: "
                            + RequestResponseMarkingFilter.class.getSimpleName() + "-inner-marker-response");
        }
    }

    /**
     * Verifies that a filter on the innermost route (the one targeting a real cluster,
     * not another router) can send OOB requests correctly. Every other OOB test in this
     * class sends the OOB from the outer route's filter; this test attaches the OOB
     * sender at the deepest level instead, so the OOB frame never needs to pass through
     * any {@code RoutingHandler} at all — it is dispatched directly by the inner route's
     * dispatcher, the same mechanism a single-level route filter would use. This exercises
     * route-name tagging surviving two layers of {@code RouteFilterHandler} wrapping on the
     * way back to the client.
     *
     * <p>Topology:
     * <pre>
     * Client → demo VC
     *   → outer router (PassThrough → "to-inner")
     *   → outer route "to-inner" [no filters]
     *   → inner router (DynamicProduce → "backend")
     *   → inner route "backend" [RequestResponseMarkingFilter, sends OOB LIST_GROUPS]
     *   → mock server
     * </pre>
     */
    @Test
    void innerRouteFilterShouldHandleOobRequestDirectly() {
        // Given
        var filterName = "inner-oob-filter";
        var filterDef = new NamedFilterDefinitionBuilder(filterName, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(LIST_TRANSACTIONS),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.REQUEST,
                                RequestResponseMarkingFilterFactory.Direction.RESPONSE),
                        "name", filterName,
                        "forwardingStyle", ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER)
                .build();

        var innerRoute = new RouteDefinition("backend", 0, List.of(filterName), new RouteTarget("mock-cluster", null));
        var innerRouter = new RouterDefinition("inner",
                DynamicProduceRouterFactory.class.getName(),
                new DynamicProduceRouterFactory.Config("backend"),
                List.of(innerRoute));

        var outerRoute = new RouteDefinition("to-inner", 0, List.of(), new RouteTarget(null, "inner"));
        var outerRouter = new RouterDefinition("outer",
                PassThroughRouterFactory.class.getName(),
                new PassThroughRouterFactory.Config("to-inner"),
                List.of(outerRoute));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "outer"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(OS_ASSIGNED_BOOTSTRAP).build())
                .build();

        try (var tester = KroxyliciousTesters.mockKafkaKroxyliciousTester(s -> {
            var clusterDef = new ClusterDefinition("mock-cluster", s, null);
            return baseConfigurationBuilder()
                    .addToClusterDefinitions(clusterDef)
                    .addToFilterDefinitions(filterDef)
                    .addToRouterDefinitions(outerRouter, innerRouter)
                    .addToVirtualClusters(vc);
        }, ROUTING_ENABLED);
                var client = tester.simpleTestClient()) {

            ApiVersionsResponseData apiVersions = new ApiVersionsResponseData();
            apiVersions.apiKeys().add(new ApiVersionsResponseData.ApiVersion()
                    .setApiKey(FETCH.id).setMaxVersion(FETCH.latestVersion()).setMinVersion(FETCH.oldestVersion()));
            tester.addMockResponseForApiKey(new ResponsePayload(API_VERSIONS, API_VERSIONS.latestVersion(), apiVersions));
            tester.addMockResponseForApiKey(new ResponsePayload(LIST_TRANSACTIONS, LIST_TRANSACTIONS.latestVersion(), new ListTransactionsResponseData()));
            tester.addMockResponseForApiKey(new ResponsePayload(LIST_GROUPS, LIST_GROUPS.latestVersion(), new ListGroupsResponseData()));

            // When
            Response response = client.getSync(new Request(LIST_TRANSACTIONS, LIST_TRANSACTIONS.latestVersion(), "client", new ListTransactionsRequestData()));

            // Then
            var requestAtBroker = tester.getOnlyRequestForApiKey(LIST_TRANSACTIONS).message();
            var responseAtClient = response.payload().message();
            assertThat(unknownTaggedFieldsToStrings(requestAtBroker, FILTER_NAME_TAG))
                    .as("inner route filter's OOB must complete before the request is forwarded to the broker")
                    .containsExactly(RequestResponseMarkingFilter.class.getSimpleName() + "-" + filterName + "-request");
            assertThat(unknownTaggedFieldsToStrings(responseAtClient, FILTER_NAME_TAG))
                    .as("inner route filter's response marking must survive the trip back through the outer route")
                    .containsExactly(RequestResponseMarkingFilter.class.getSimpleName() + "-" + filterName + "-response");
        }
    }

    /**
     * Verifies that a VC-level filter (not scoped to any route) can send OOB requests
     * correctly when the VC's target is a nested router, rather than a plain cluster.
     * The OOB originates above the outer {@code RoutingHandler} entirely, so the outer
     * router's {@code onRequest} forwards it down to the nested router like a regular
     * (non-OOB) request — a different path than a route-scoped filter's OOB, which is
     * tagged with its own route name from the moment it is created.
     *
     * <p>The outer router must implement {@code onRequest} for this to work: OOB frames
     * always bypass static routing, so the outer router's {@code onRequest} is invoked
     * for the OOB's API key even though it is otherwise handled by {@code staticRoutes()}.
     *
     * <p>Topology:
     * <pre>
     * Client → demo VC [OutOfBandSendFilter]
     *   → outer router (DynamicProduce → "to-inner")
     *   → outer route "to-inner" [no filters]
     *   → inner router (DynamicProduce → "backend")
     *   → inner route "backend" [RequestResponseMarkingFilter]
     *   → mock server
     * </pre>
     */
    @Test
    void vcFilterShouldHandleOobRequestWhenTargetIsNestedRouter() {
        // Given
        var vcOobFilter = new NamedFilterDefinitionBuilder("oob-sender", OutOfBandSendFilterFactory.class.getName())
                .withConfig(Map.of("apiKeyToSend", CREATE_TOPICS, "tagToCollect", FILTER_NAME_TAG))
                .build();
        var innerMarkerFilter = new NamedFilterDefinitionBuilder("inner-marker", RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("name", "inner-marker", "keysToMark", Set.of(CREATE_TOPICS))
                .build();

        var innerRoute = new RouteDefinition("backend", 0, List.of("inner-marker"), new RouteTarget("mock-cluster", null));
        var innerRouter = new RouterDefinition("inner",
                DynamicProduceRouterFactory.class.getName(),
                new DynamicProduceRouterFactory.Config("backend"),
                List.of(innerRoute));

        var outerRoute = new RouteDefinition("to-inner", 0, List.of(), new RouteTarget(null, "inner"));
        var outerRouter = new RouterDefinition("outer",
                DynamicProduceRouterFactory.class.getName(),
                new DynamicProduceRouterFactory.Config("to-inner"),
                List.of(outerRoute));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "outer"))
                .withFilters(List.of("oob-sender"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(OS_ASSIGNED_BOOTSTRAP).build())
                .build();

        try (var tester = KroxyliciousTesters.mockKafkaKroxyliciousTester(s -> {
            var clusterDef = new ClusterDefinition("mock-cluster", s, null);
            return baseConfigurationBuilder()
                    .addToClusterDefinitions(clusterDef)
                    .addToFilterDefinitions(vcOobFilter, innerMarkerFilter)
                    .addToRouterDefinitions(outerRouter, innerRouter)
                    .addToVirtualClusters(vc);
        }, ROUTING_ENABLED);
                var client = tester.simpleTestClient()) {

            tester.addMockResponseForApiKey(new ResponsePayload(CREATE_TOPICS, CREATE_TOPICS.latestVersion(), arbitraryCreateTopicsResponse()));
            tester.addMockResponseForApiKey(new ResponsePayload(DESCRIBE_CLUSTER, DESCRIBE_CLUSTER.latestVersion(), arbitraryDescribeClusterResponse()));

            // When
            var response = client.getSync(
                    new Request(DESCRIBE_CLUSTER, DESCRIBE_CLUSTER.latestVersion(), "client", new DescribeClusterRequestData()));

            // Then
            var responseData = (DescribeClusterResponseData) response.payload().message();
            assertThat(responseData.errorMessage()).isEqualTo(
                    "filterNameTaggedFieldsFromOutOfBandResponse: "
                            + RequestResponseMarkingFilter.class.getSimpleName() + "-inner-marker-response");
        }
    }

    private static CreateTopicsResponseData arbitraryCreateTopicsResponse() {
        var message = new CreateTopicsResponseData();
        var topic = new CreateTopicsResponseData.CreatableTopicResult();
        topic.setName("mockTopic");
        topic.setNumPartitions(3);
        topic.setReplicationFactor((short) 3);
        message.topics().add(topic);
        return message;
    }

    private static DescribeClusterResponseData arbitraryDescribeClusterResponse() {
        var message = new DescribeClusterResponseData();
        message.setErrorMessage("arbitrary");
        message.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        return message;
    }
}
