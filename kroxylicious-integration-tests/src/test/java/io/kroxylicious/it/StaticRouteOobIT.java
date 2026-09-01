/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.util.List;
import java.util.Map;
import java.util.Set;

import io.kroxylicious.kafka.common.message.CreateTopicsResponseData;
import io.kroxylicious.kafka.common.message.DescribeClusterRequestData;
import io.kroxylicious.kafka.common.message.DescribeClusterResponseData;
import io.kroxylicious.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.OutOfBandSendFilterFactory;
import io.kroxylicious.it.testplugins.RequestResponseMarkingFilter;
import io.kroxylicious.it.testplugins.RequestResponseMarkingFilterFactory;
import io.kroxylicious.it.testplugins.router.PassThroughRouterFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
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

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.OS_ASSIGNED_BOOTSTRAP;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.CREATE_TOPICS;
import static io.kroxylicious.kafka.common.protocol.ApiKeys.DESCRIBE_CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that an OOB request whose API key is declared in a router's {@code staticRoutes()} is
 * forwarded via the same static-route path as an ordinary request, rather than being forced through
 * {@code Router.onRequest()}. Uses {@link PassThroughRouterFactory}, whose {@code onRequest()}
 * throws unconditionally on the assumption (correct, per the {@code Router} javadoc) that a fully
 * static router's {@code onRequest} is never called — before this fix, an OOB request would have
 * forced that call and crashed the router.
 */
@ExtendWith(NettyLeakDetectorExtension.class)
class StaticRouteOobIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();

    @Test
    void oobForStaticallyRoutedApiKeyShouldNotInvokeOnRequest() {
        // Given
        var oobFilter = new NamedFilterDefinitionBuilder("oob-sender", OutOfBandSendFilterFactory.class.getName())
                .withConfig(Map.of("apiKeyToSend", CREATE_TOPICS, "tagToCollect", RequestResponseMarkingFilter.FILTER_NAME_TAG))
                .build();
        var markerFilter = new NamedFilterDefinitionBuilder("single-marker", RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("name", "single-marker", "keysToMark", Set.of(CREATE_TOPICS))
                .build();

        var route = new RouteDefinition("route-a", 0, List.of("oob-sender", "single-marker"), new RouteTarget("mock-cluster", null));
        var router = new RouterDefinition("single",
                PassThroughRouterFactory.class.getName(),
                new PassThroughRouterFactory.Config("route-a"),
                List.of(route));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "single"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(OS_ASSIGNED_BOOTSTRAP).build())
                .build();

        try (var tester = KroxyliciousTesters.mockKafkaKroxyliciousTester(s -> {
            var clusterDef = new ClusterDefinition("mock-cluster", s, null);
            return baseConfigurationBuilder()
                    .addToClusterDefinitions(clusterDef)
                    .addToFilterDefinitions(oobFilter, markerFilter)
                    .addToRouterDefinitions(router)
                    .addToVirtualClusters(vc);
        }, ROUTING_ENABLED);
                var client = tester.simpleTestClient()) {

            tester.addMockResponseForApiKey(new ResponsePayload(CREATE_TOPICS, CREATE_TOPICS.latestVersion(), arbitraryCreateTopicsResponse()));
            tester.addMockResponseForApiKey(new ResponsePayload(DESCRIBE_CLUSTER, DESCRIBE_CLUSTER.latestVersion(), arbitraryDescribeClusterResponse()));

            // When
            var response = client.getSync(
                    new Request(DESCRIBE_CLUSTER, DESCRIBE_CLUSTER.latestVersion(), "client", new DescribeClusterRequestData()));

            // Then: PassThroughRouterFactory.onRequest() throws unconditionally, so completing this
            // response at all proves the OOB never invoked it — the router's onRequest is unsupported.
            var responseData = (DescribeClusterResponseData) response.payload().message();
            assertThat(responseData.errorMessage()).isEqualTo(
                    "filterNameTaggedFieldsFromOutOfBandResponse: "
                            + RequestResponseMarkingFilter.class.getSimpleName() + "-single-marker-response");
        }
    }

    /**
     * Same as above, but nested: the OOB's API key is statically routed at both the outer and the
     * inner router, so neither router's {@code onRequest()} — both {@link PassThroughRouterFactory},
     * both throwing unconditionally — should ever be invoked.
     *
     * <p>Topology:
     * <pre>
     * Client → demo VC
     *   → outer router (PassThrough → "to-inner")
     *   → outer route "to-inner" [OutOfBandSendFilter]
     *   → inner router (PassThrough → "backend")
     *   → inner route "backend" [RequestResponseMarkingFilter]
     *   → mock server
     * </pre>
     */
    @Test
    void nestedOobForStaticallyRoutedApiKeyShouldNotInvokeEitherRoutersOnRequest() {
        // Given
        var outerOobFilter = new NamedFilterDefinitionBuilder("oob-sender", OutOfBandSendFilterFactory.class.getName())
                .withConfig(Map.of("apiKeyToSend", CREATE_TOPICS, "tagToCollect", RequestResponseMarkingFilter.FILTER_NAME_TAG))
                .build();
        var innerMarkerFilter = new NamedFilterDefinitionBuilder("inner-marker", RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("name", "inner-marker", "keysToMark", Set.of(CREATE_TOPICS))
                .build();

        var innerRoute = new RouteDefinition("backend", 0, List.of("inner-marker"), new RouteTarget("mock-cluster", null));
        var innerRouter = new RouterDefinition("inner",
                PassThroughRouterFactory.class.getName(),
                new PassThroughRouterFactory.Config("backend"),
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
