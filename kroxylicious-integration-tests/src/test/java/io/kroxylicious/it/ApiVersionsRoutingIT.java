/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.net.URI;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.internal.routing.RoutingEvent;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.Response;
import io.kroxylicious.testing.integration.client.KafkaClient;

import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for API_VERSIONS handling in the topic-partition router.
 */
class ApiVersionsRoutingIT extends TopicPartitionRoutingBaseIT {

    @Test
    void shouldNotCapTopicIdBearingApiVersions() throws Exception {
        createTopic("a.dummy", clusterA);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            var address = URI.create("kafka://" + tester.getBootstrapAddress());
            try (var client = new KafkaClient(address.getHost(), address.getPort())) {
                var request = new Request(
                        ApiKeys.API_VERSIONS,
                        ApiKeys.API_VERSIONS.latestVersion(),
                        "test-client",
                        new ApiVersionsRequestData()
                                .setClientSoftwareName("test")
                                .setClientSoftwareVersion("1.0"));

                Response response = client.getSync(request);
                var body = (ApiVersionsResponseData) response.payload().message();

                var produceVersion = body.apiKeys().find(ApiKeys.PRODUCE.id);
                assertThat(produceVersion).isNotNull();
                assertThat(produceVersion.maxVersion())
                        .as("PRODUCE should not be capped — topicId resolution handles v13+")
                        .isGreaterThanOrEqualTo((short) 13);

                var fetchVersion = body.apiKeys().find(ApiKeys.FETCH.id);
                assertThat(fetchVersion).isNotNull();
                assertThat(fetchVersion.maxVersion())
                        .as("FETCH should not be capped — topicId resolution handles v13+")
                        .isGreaterThanOrEqualTo((short) 13);

                var offsetCommitVersion = body.apiKeys().find(ApiKeys.OFFSET_COMMIT.id);
                assertThat(offsetCommitVersion).isNotNull();
                assertThat(offsetCommitVersion.maxVersion())
                        .as("OFFSET_COMMIT should not be capped — topicId resolution handles v10+")
                        .isGreaterThanOrEqualTo((short) 10);

                var offsetFetchVersion = body.apiKeys().find(ApiKeys.OFFSET_FETCH.id);
                assertThat(offsetFetchVersion).isNotNull();
                assertThat(offsetFetchVersion.maxVersion())
                        .as("OFFSET_FETCH should not be capped — topicId resolution handles v10+")
                        .isGreaterThanOrEqualTo((short) 10);

                var deleteTopicsVersion = body.apiKeys().find(ApiKeys.DELETE_TOPICS.id);
                assertThat(deleteTopicsVersion).isNotNull();
                assertThat(deleteTopicsVersion.maxVersion())
                        .as("DELETE_TOPICS should not be capped — topicId resolution handles v6+")
                        .isGreaterThanOrEqualTo((short) 6);

                var addPartitionsVersion = body.apiKeys().find(ApiKeys.ADD_PARTITIONS_TO_TXN.id);
                assertThat(addPartitionsVersion).isNotNull();
                assertThat(addPartitionsVersion.maxVersion())
                        .as("ADD_PARTITIONS_TO_TXN should still be capped (not topicId-related)")
                        .isLessThanOrEqualTo((short) 3);

                var findCoordinatorVersion = body.apiKeys().find(ApiKeys.FIND_COORDINATOR.id);
                assertThat(findCoordinatorVersion).isNotNull();
                assertThat(findCoordinatorVersion.maxVersion())
                        .as("FIND_COORDINATOR should still be capped (not topicId-related)")
                        .isLessThanOrEqualTo((short) 3);
            }
        }

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.API_VERSIONS))
                .as("API_VERSIONS should be routed to default route")
                .hasSize(1);
    }

    @Test
    void shouldRouteApiVersionsToBrokerSpecificEndpoint() throws Exception {
        // Given
        createTopic("a.broker-specific", clusterA);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            var bootstrapAddress = URI.create("kafka://" + tester.getBootstrapAddress());
            try (var bootstrapClient = new KafkaClient(
                    bootstrapAddress.getHost(), bootstrapAddress.getPort())) {
                negotiateApiVersions(bootstrapClient);
                var metadata = fetchMetadata(bootstrapClient, "a.broker-specific");

                // When: connect to a broker-specific endpoint
                try (var brokerClient = clientForLeader(
                        tester, metadata, "a.broker-specific", 0)) {
                    // clientForLeader calls negotiateApiVersions, which sends API_VERSIONS
                    // to the broker-specific endpoint

                    // Then: find the API_VERSIONS events
                    var apiVersionEvents = routingCaptor.requestEvents().stream()
                            .filter(e -> e.apiKey() == ApiKeys.API_VERSIONS)
                            .toList();

                    assertThat(apiVersionEvents)
                            .as("should have at least 2 API_VERSIONS events "
                                    + "(one bootstrap, one broker-specific)")
                            .hasSizeGreaterThanOrEqualTo(2);

                    // The broker-specific event should have a non-negative virtualNodeId
                    // (negative values are bootstrap/anyNode sentinels)
                    RoutingEvent.Request brokerSpecificEvent = apiVersionEvents.get(apiVersionEvents.size() - 1);
                    assertThat(brokerSpecificEvent.virtualNodeId())
                            .as("broker-specific API_VERSIONS should target a real node, "
                                    + "not a bootstrap sentinel")
                            .isGreaterThanOrEqualTo(0);
                }
            }
        }
    }
}
