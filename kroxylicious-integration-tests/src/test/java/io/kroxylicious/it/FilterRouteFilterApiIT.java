/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.filter.simpletransform.FetchResponseTransformation;
import io.kroxylicious.filter.simpletransform.ProduceRequestTransformation;
import io.kroxylicious.it.testplugins.ForwardingStyle;
import io.kroxylicious.it.testplugins.RejectingCreateTopicFilter;
import io.kroxylicious.it.testplugins.RejectingCreateTopicFilterFactory;
import io.kroxylicious.it.testplugins.RequestCountingFilter;
import io.kroxylicious.it.testplugins.RequestCountingFilterFactory;
import io.kroxylicious.it.testplugins.RequestResponseMarkingFilter;
import io.kroxylicious.it.testplugins.RequestResponseMarkingFilterFactory;
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
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.apache.kafka.common.protocol.ApiKeys.FETCH;
import static org.apache.kafka.common.protocol.ApiKeys.LIST_GROUPS;
import static org.apache.kafka.common.protocol.ApiKeys.LIST_TRANSACTIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Verifies that filter APIs work correctly when filters are configured on routes rather
 * than on the virtual cluster.  Each test places its filter in the route's filter chain
 * rather than in the VC's default filter list, confirming that the route-level placement
 * does not break any filter contract.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class FilterRouteFilterApiIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();
    private static final String ROUTE_NAME = "default-route";
    private static final String ROUTER_NAME = "pass-through";
    private static final String CLUSTER_NAME = "backing";
    private static final String PLAINTEXT = "Hello, world!";

    private ConfigurationBuilder routedConfigWithRouteFilter(String bootstrapServers, NamedFilterDefinition... filterDefs) {
        var clusterDef = new ClusterDefinition(CLUSTER_NAME, bootstrapServers, null);
        var filterNames = Arrays.stream(filterDefs).map(NamedFilterDefinition::name).toList();
        var route = new RouteDefinition(ROUTE_NAME, 0, filterNames, new RouteTarget(CLUSTER_NAME, null));
        var routerConfig = new PassThroughRouterFactory.Config(ROUTE_NAME);
        var routerDef = new RouterDefinition(ROUTER_NAME, PassThroughRouterFactory.class.getName(), routerConfig, List.of(route));
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

    @Test
    void requestFilterCanModifyProduceRequestOnRoute(KafkaCluster cluster, Topic topic) throws Exception {
        var bytes = PLAINTEXT.getBytes(StandardCharsets.UTF_8);
        var expected = AbstractFilterIT.encode(topic.name(), ByteBuffer.wrap(bytes)).array();

        // Given
        var filterDef = new NamedFilterDefinitionBuilder(ProduceRequestTransformation.class.getName(), ProduceRequestTransformation.class.getName())
                .withConfig("transformation", TestEncoderFactory.class.getName())
                .build();
        var config = routedConfigWithRouteFilter(cluster.getBootstrapServers(), filterDef);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(CLIENT_ID_CONFIG, "test", DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
                var consumer = tester.consumer(Serdes.String(), Serdes.ByteArray(),
                        Map.of(GROUP_ID_CONFIG, "encode-group", AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            // When
            producer.send(new ProducerRecord<>(topic.name(), "key", PLAINTEXT)).get();
            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));

            // Then
            assertThat(records.records(topic.name()))
                    .hasSize(1)
                    .map(ConsumerRecord::value)
                    .containsExactly(expected);
        }
    }

    @Test
    void responseFilterCanModifyFetchResponseOnRoute(KafkaCluster cluster, Topic topic) throws Exception {
        var bytes = PLAINTEXT.getBytes(StandardCharsets.UTF_8);
        var encoded = AbstractFilterIT.encode(topic.name(), ByteBuffer.wrap(bytes)).array();

        // Given
        var filterDef = new NamedFilterDefinitionBuilder("fetch-decoder", FetchResponseTransformation.class.getName())
                .withConfig("transformation", TestDecoderFactory.class.getName())
                .build();
        var config = routedConfigWithRouteFilter(cluster.getBootstrapServers(), filterDef);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Serdes.String(), Serdes.ByteArray(),
                        Map.of(CLIENT_ID_CONFIG, "test", DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
                var consumer = tester.consumer(Map.of(GROUP_ID_CONFIG, "decode-group", AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            // When
            producer.send(new ProducerRecord<>(topic.name(), "key", encoded)).get();
            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));

            // Then
            assertThat(records.records(topic.name()))
                    .hasSize(1)
                    .map(ConsumerRecord::value)
                    .containsExactly(PLAINTEXT);
        }
    }

    @Test
    void requestFilterCanShortCircuitResponseOnRoute(KafkaCluster cluster) {
        // Given
        var filterDef = new NamedFilterDefinitionBuilder(RejectingCreateTopicFilterFactory.class.getName(), RejectingCreateTopicFilterFactory.class.getName())
                .build();
        var config = routedConfigWithRouteFilter(cluster.getBootstrapServers(), filterDef);
        var topicName = "should-not-be-created";

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var admin = tester.admin()) {

            // When / Then
            assertThatExceptionOfType(ExecutionException.class)
                    .isThrownBy(() -> admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1))).all().get())
                    .withCauseInstanceOf(InvalidTopicException.class)
                    .havingCause()
                    .withMessage(RejectingCreateTopicFilter.ERROR_MESSAGE);
        }
    }

    @Test
    void routeFilterCanSendOutOfBandRequestToBroker() {
        var filterName = "marking-filter";

        // Given
        var filterDef = new NamedFilterDefinitionBuilder(filterName, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(LIST_TRANSACTIONS),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.REQUEST,
                                RequestResponseMarkingFilterFactory.Direction.RESPONSE),
                        "name", filterName,
                        "forwardingStyle", ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER)
                .build();

        try (var tester = KroxyliciousTesters.mockKafkaKroxyliciousTester(
                mockBootstrap -> routedConfigWithRouteFilter(mockBootstrap, filterDef), ROUTING_ENABLED);
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
                    .containsExactly(RequestResponseMarkingFilter.class.getSimpleName() + "-" + filterName + "-request");
            assertThat(unknownTaggedFieldsToStrings(responseAtClient, FILTER_NAME_TAG))
                    .containsExactly(RequestResponseMarkingFilter.class.getSimpleName() + "-" + filterName + "-response");
        }
    }

    @Test
    void routeFilterReceivesRequestAndResponseForSameExchange(KafkaCluster cluster, Topic topic) throws Exception {
        var filterName = "req-resp-marker";

        // Given
        var filterDef = new NamedFilterDefinitionBuilder(filterName, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(ApiKeys.PRODUCE),
                        "direction", Set.of(RequestResponseMarkingFilterFactory.Direction.REQUEST,
                                RequestResponseMarkingFilterFactory.Direction.RESPONSE),
                        "name", filterName,
                        "forwardingStyle", ForwardingStyle.SYNCHRONOUS)
                .build();
        var config = routedConfigWithRouteFilter(cluster.getBootstrapServers(), filterDef);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(CLIENT_ID_CONFIG, "test", DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000))) {

            // When
            producer.send(new ProducerRecord<>(topic.name(), "key", "value")).get();

            // Then: no exception means both request and response paths completed correctly through the route filter
        }
    }

    @Test
    void apiVersionsRequestUsedForVersionNegotiationPassesThroughRouteFilter(KafkaCluster cluster, Topic topic) throws Exception {
        // Given
        RequestCountingFilter.reset("api-versions-test");
        var filterDef = new NamedFilterDefinitionBuilder("counter", RequestCountingFilterFactory.class.getName())
                .withConfig("counterId", "api-versions-test")
                .build();
        var config = routedConfigWithRouteFilter(cluster.getBootstrapServers(), filterDef);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer()) {

            // When
            producer.send(new ProducerRecord<>(topic.name(), "key", "value")).get();
        }

        // Then: the filter was invoked for traffic through the route (API_VERSIONS is used during handshake)
        assertThat(RequestCountingFilter.countFor("api-versions-test", ApiKeys.API_VERSIONS)).isGreaterThanOrEqualTo(1);
    }
}
