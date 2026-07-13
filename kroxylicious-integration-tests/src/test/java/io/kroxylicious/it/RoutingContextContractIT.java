/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.router.ContextCapturingRouterFactory;
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
import io.kroxylicious.proxy.topology.VirtualNode;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.Response;
import io.kroxylicious.testing.integration.ResponsePayload;
import io.kroxylicious.testing.integration.server.MockServer;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@link io.kroxylicious.proxy.router.RouterContext} contract: each method
 * behaves correctly across different connection types and produces the documented outcome.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RoutingContextContractIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();
    private static final String ROUTE = "backing-route";
    private static final String ROUTER = "contract-router";
    private static final String CLUSTER = "backing-cluster";
    private static final String BOOTSTRAP = "localhost:9192";

    @BeforeEach
    @AfterEach
    void resetCapture() {
        ContextCapturingRouterFactory.reset();
    }

    private ConfigurationBuilder config(KafkaCluster cluster) {
        return config("localhost:" + cluster.getBootstrapServers().split(":")[1]);
    }

    private ConfigurationBuilder config(String upstreamBootstrap) {
        return configWithNodeRange(upstreamBootstrap, 0, 0);
    }

    private ConfigurationBuilder configWithNodeRange(String upstreamBootstrap, int low, int high) {
        var clusterDef = new ClusterDefinition(CLUSTER, upstreamBootstrap, null);
        var route = new RouteDefinition(ROUTE, 0, List.of(), new RouteTarget(CLUSTER, null));
        var routerDef = new RouterDefinition(ROUTER,
                ContextCapturingRouterFactory.class.getName(),
                new ContextCapturingRouterFactory.Config(ROUTE),
                List.of(route));
        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, ROUTER))
                .addToGateways(defaultGatewayBuilder()
                        .withNewPortIdentifiesNode()
                        .withBootstrapAddress(HostPort.parse(BOOTSTRAP))
                        .withNodeIdRanges(new NamedRange("nodes", low, high))
                        .endPortIdentifiesNode()
                        .build())
                .build();
        return baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }

    @Test
    void virtualNodeIsEmptyForBootstrapConnection(KafkaCluster cluster) {
        // Given: router factory with no static routes (everything dynamic)
        try (var tester = KroxyliciousTesters.newBuilder(config(cluster))
                .setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var client = tester.simpleTestClient()) {

            // When: request arrives via the bootstrap port
            client.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));

            // Then: bootstrap connections have no associated virtual node
            assertThat(ContextCapturingRouterFactory.lastCapture.get().virtualNode()).isEmpty();
        }
    }

    @Test
    void virtualNodeIsPresentForBrokerSpecificConnection() {
        // Given: a single mock upstream with METADATA that advertises two brokers so the
        // proxy allocates ports 9193 (virtual node 0) and 9194 (virtual node 1).
        try (var mockBroker0 = MockServer.startOnRandomPort();
                var mockBroker1 = MockServer.startOnRandomPort()) {

            var md = new MetadataResponseData();
            md.brokers().add(new MetadataResponseBroker().setNodeId(0).setHost("localhost").setPort(mockBroker0.port()));
            md.brokers().add(new MetadataResponseBroker().setNodeId(1).setHost("localhost").setPort(mockBroker1.port()));
            mockBroker0.addMockResponseForApiKey(new ResponsePayload(ApiKeys.METADATA, (short) 12, md));

            try (var tester = KroxyliciousTesters.newBuilder(configWithNodeRange("localhost:" + mockBroker0.port(), 0, 1))
                    .setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester()) {

                // Prime reconciliation so port 9193 is bound to mockBroker0
                try (var bootstrap = tester.simpleTestClient()) {
                    bootstrap.getSync(new Request(ApiKeys.METADATA, (short) 12, "client", new MetadataRequestData()));
                }
                mockBroker0.clear();
                mockBroker1.clear();
                mockBroker0.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, new ApiVersionsResponseData()));

                // When: connect to the broker-specific port for virtual node 0
                try (var nodeZero = tester.simpleTestClient("localhost:9193", false)) {
                    nodeZero.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));
                }

                // Then: a broker-specific connection has a non-empty virtualNode
                assertThat(ContextCapturingRouterFactory.lastCapture.get().virtualNode()).isPresent();
            }
        }
    }

    @Test
    void sendingToVirtualNodeRoutesToTheConnectedBroker() {
        // Given: two mock brokers; connect to the broker-specific port for virtual node 0
        // and have the router use ctx.virtualNode() to route the request.
        try (var mockBroker0 = MockServer.startOnRandomPort();
                var mockBroker1 = MockServer.startOnRandomPort()) {

            var apiVersionsResponse = new ApiVersionsResponseData();
            mockBroker0.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, apiVersionsResponse));
            mockBroker1.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, apiVersionsResponse));

            var md = new MetadataResponseData();
            md.brokers().add(new MetadataResponseBroker().setNodeId(0).setHost("localhost").setPort(mockBroker0.port()));
            md.brokers().add(new MetadataResponseBroker().setNodeId(1).setHost("localhost").setPort(mockBroker1.port()));
            mockBroker0.addMockResponseForApiKey(new ResponsePayload(ApiKeys.METADATA, (short) 12, md));

            // Router: for any non-METADATA request, use virtualNode() to route to the connected broker
            ContextCapturingRouterFactory.currentAction.set((apiKey, apiVersion, header, request, ctx) -> {
                var vn = ctx.virtualNode();
                if (vn.isPresent()) {
                    return ctx.sendRequest(vn.get(), header, request)
                            .thenCompose(body -> ctx.respondWith(body).completed());
                }
                return ctx.sendRequest(ctx.anyNode(ROUTE), header, request)
                        .thenCompose(body -> ctx.respondWith(body).completed());
            });

            try (var tester = KroxyliciousTesters.newBuilder(configWithNodeRange("localhost:" + mockBroker0.port(), 0, 1))
                    .setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester()) {

                try (var bootstrap = tester.simpleTestClient()) {
                    bootstrap.getSync(new Request(ApiKeys.METADATA, (short) 12, "client", new MetadataRequestData()));
                }
                mockBroker0.clear();
                mockBroker1.clear();
                mockBroker0.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, new ApiVersionsResponseData()));
                mockBroker1.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, new ApiVersionsResponseData()));

                // When: connect to virtual-node-0 port and send a request
                try (var nodeZero = tester.simpleTestClient("localhost:9193", false)) {
                    nodeZero.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));
                }

                // When: connect to virtual-node-1 port and send a request
                try (var nodeOne = tester.simpleTestClient("localhost:9194", false)) {
                    nodeOne.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));
                }

                // Then: each virtual-node connection routed to the corresponding upstream broker
                assertThat(mockBroker0.getReceivedRequests())
                        .extracting(Request::apiKeys)
                        .containsExactly(ApiKeys.API_VERSIONS);
                assertThat(mockBroker1.getReceivedRequests())
                        .extracting(Request::apiKeys)
                        .containsExactly(ApiKeys.API_VERSIONS);
            }
        }
    }

    @Test
    void nodeForIdReturnsNonNullVirtualNodeForKnownId(KafkaCluster cluster) {
        // Given
        try (var tester = KroxyliciousTesters.newBuilder(config(cluster))
                .setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var client = tester.simpleTestClient()) {

            // When
            client.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));

            // Then: nodeForId(0) returns a VirtualNode (identity mapping: virtual 0 → route broker 0)
            assertThat(ContextCapturingRouterFactory.lastCapture.get().nodeForIdZero())
                    .as("nodeForId(0) must return a non-null VirtualNode for a single-route setup")
                    .isNotNull()
                    .isInstanceOf(VirtualNode.class);
        }
    }

    @Test
    void nodeForIdCanBeUsedToTargetASpecificBroker() {
        // Given: two mock brokers; after reconciliation, the router uses nodeForId(0)
        // to target virtual node 0's upstream directly.
        try (var mockBroker0 = MockServer.startOnRandomPort();
                var mockBroker1 = MockServer.startOnRandomPort()) {

            mockBroker0.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, new ApiVersionsResponseData()));
            var md = new MetadataResponseData();
            md.brokers().add(new MetadataResponseBroker().setNodeId(0).setHost("localhost").setPort(mockBroker0.port()));
            md.brokers().add(new MetadataResponseBroker().setNodeId(1).setHost("localhost").setPort(mockBroker1.port()));
            mockBroker0.addMockResponseForApiKey(new ResponsePayload(ApiKeys.METADATA, (short) 12, md));

            // Router: send via the VirtualNode returned by nodeForId(0), always to broker 0
            ContextCapturingRouterFactory.currentAction.set((apiKey, apiVersion, header, request, ctx) -> {
                if (apiKey == ApiKeys.API_VERSIONS) {
                    var node0 = ctx.nodeForId(0);
                    return ctx.sendRequest(node0, header, request)
                            .thenCompose(body -> ctx.respondWith(body).completed());
                }
                return ctx.sendRequest(ctx.anyNode(ROUTE), header, request)
                        .thenCompose(body -> ctx.respondWith(body).completed());
            });
            mockBroker1.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, new ApiVersionsResponseData()));

            try (var tester = KroxyliciousTesters.newBuilder(configWithNodeRange("localhost:" + mockBroker0.port(), 0, 1))
                    .setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester()) {

                // Prime METADATA reconciliation from bootstrap
                try (var bootstrap = tester.simpleTestClient()) {
                    bootstrap.getSync(new Request(ApiKeys.METADATA, (short) 12, "client", new MetadataRequestData()));
                }
                mockBroker0.clear();
                mockBroker0.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, new ApiVersionsResponseData()));

                // When: send API_VERSIONS — router directs it to nodeForId(0) = broker 0
                try (var client = tester.simpleTestClient()) {
                    client.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));
                }

                // Then: broker 0 received the request; broker 1 did not
                assertThat(mockBroker0.getReceivedRequests())
                        .extracting(Request::apiKeys)
                        .containsExactly(ApiKeys.API_VERSIONS);
                assertThat(mockBroker1.getReceivedRequests()).isEmpty();
            }
        }
    }

    @Test
    void sessionIdIsNonNullAndNonEmpty(KafkaCluster cluster) {
        // Given
        try (var tester = KroxyliciousTesters.newBuilder(config(cluster))
                .setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var client = tester.simpleTestClient()) {

            // When
            client.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));

            // Then
            assertThat(ContextCapturingRouterFactory.lastCapture.get().sessionId())
                    .isNotNull()
                    .isNotEmpty();
        }
    }

    @Test
    void authenticatedSubjectIsAvailableAndAnonymousForPlaintextConnection(KafkaCluster cluster) {
        // Given
        try (var tester = KroxyliciousTesters.newBuilder(config(cluster))
                .setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var client = tester.simpleTestClient()) {

            // When
            client.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));

            // Then: plaintext (no mutual TLS) → anonymous subject
            var subject = ContextCapturingRouterFactory.lastCapture.get().authenticatedSubject();
            assertThat(subject).isNotNull();
            assertThat(subject.isAnonymous()).isTrue();
        }
    }

    @Test
    void respondWithBodyDeliversCustomResponseToClient(KafkaCluster cluster) {
        // Given: router synthesises an API_VERSIONS response with a known set of API keys
        var customResponse = new ApiVersionsResponseData();
        customResponse.apiKeys().add(new ApiVersionsResponseData.ApiVersion()
                .setApiKey(ApiKeys.PRODUCE.id).setMinVersion((short) 0).setMaxVersion((short) 9));

        ContextCapturingRouterFactory.currentAction.set((apiKey, apiVersion, header, request, ctx) -> ctx.respondWith(customResponse).completed());

        try (var tester = KroxyliciousTesters.newBuilder(config(cluster))
                .setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var client = tester.simpleTestClient()) {

            // When
            Response response = client.getSync(
                    new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));

            // Then: the custom body is delivered to the client
            assertThat(response.payload().message()).isInstanceOf(ApiVersionsResponseData.class);
            var data = (ApiVersionsResponseData) response.payload().message();
            assertThat(data.apiKeys()).hasSize(1);
            assertThat(data.apiKeys().find(ApiKeys.PRODUCE.id).maxVersion()).isEqualTo((short) 9);
        }
    }

    @Test
    void respondWithExplicitHeaderDeliversResponseWithCustomHeader(KafkaCluster cluster) {
        // Given: router provides an explicit response header with an unknown tagged field.
        // API_VERSIONS always uses response header v0 (no tagged fields); LIST_GROUPS v3+ uses
        // flexible versioning (response header v1) so tagged fields can be encoded.
        var customBody = new ListGroupsResponseData();

        ContextCapturingRouterFactory.currentAction.set((apiKey, apiVersion, header, request, ctx) -> {
            var customHeader = new ResponseHeaderData();
            customHeader.unknownTaggedFields().add(new RawTaggedField(99, new byte[]{ 0x42 }));
            return ctx.respondWith(customHeader, customBody).completed();
        });

        try (var tester = KroxyliciousTesters.newBuilder(config(cluster))
                .setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var client = tester.simpleTestClient()) {

            // When: the client receives a response built with a custom header
            Response response = client.getSync(
                    new Request(ApiKeys.LIST_GROUPS, (short) 3, "client", new ListGroupsRequestData()));

            // Then: the response body is the one the router set; the header tagged field did not
            // cause an encoding error (the Kafka client silently ignores unknown header tags)
            assertThat(response.payload().message()).isInstanceOf(ListGroupsResponseData.class);
        }
    }

    @Test
    void respondWithErrorDeliversApiSpecificErrorResponseToClient(KafkaCluster cluster) {
        // Given: router returns an error for every API_VERSIONS request
        ContextCapturingRouterFactory.currentAction
                .set((apiKey, apiVersion, header, request, ctx) -> ctx.respondWithError(header, request, new UnknownServerException("routing failed")).completed());

        try (var tester = KroxyliciousTesters.newBuilder(config(cluster))
                .setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var client = tester.simpleTestClient()) {

            // When
            Response response = client.getSync(
                    new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));

            // Then: the runtime synthesised an API_VERSIONS error response
            assertThat(response.payload().message()).isInstanceOf(ApiVersionsResponseData.class);
            var data = (ApiVersionsResponseData) response.payload().message();
            assertThat(data.errorCode()).isEqualTo(Errors.UNKNOWN_SERVER_ERROR.code());
        }
    }

    @Test
    void respondWithoutReplyCompletesFireAndForgetProduce(KafkaCluster cluster, Topic topic) {
        // Given: router calls respondWithoutReply() for acks=0 PRODUCE; all other keys pass through
        var produceHandled = new CompletableFuture<Void>();
        ContextCapturingRouterFactory.currentAction.set((apiKey, apiVersion, header, request, ctx) -> {
            if (apiKey == ApiKeys.PRODUCE && request instanceof ProduceRequestData pd && pd.acks() == 0) {
                produceHandled.complete(null);
                return ctx.respondWithoutReply().completed();
            }
            return ctx.sendRequest(ctx.anyNode(ROUTE), header, request)
                    .thenCompose(body -> ctx.respondWith(body).completed());
        });

        try (var tester = KroxyliciousTesters.newBuilder(config(cluster))
                .setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of("acks", "0", "retries", "0", "linger.ms", "0"))) {

            // When: acks=0 producer — send() completes before any server acknowledgement
            producer.send(new ProducerRecord<>(topic.name(), "key", "value"));
            producer.flush();

            // Then: the router's onRequest was invoked and chose respondWithoutReply
            assertThat(produceHandled).succeedsWithin(Duration.ofSeconds(10));
        }
    }
}
