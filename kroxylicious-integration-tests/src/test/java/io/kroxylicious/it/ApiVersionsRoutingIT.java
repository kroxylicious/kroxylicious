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
    void shouldCapApiVersionsForTopicIdBearingKeys() throws Exception {
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
                assertThat(produceVersion.maxVersion()).isLessThanOrEqualTo((short) 12);

                var fetchVersion = body.apiKeys().find(ApiKeys.FETCH.id);
                assertThat(fetchVersion).isNotNull();
                assertThat(fetchVersion.maxVersion()).isLessThanOrEqualTo((short) 12);

                var offsetCommitVersion = body.apiKeys().find(ApiKeys.OFFSET_COMMIT.id);
                assertThat(offsetCommitVersion).isNotNull();
                assertThat(offsetCommitVersion.maxVersion()).isLessThanOrEqualTo((short) 9);

                var offsetFetchVersion = body.apiKeys().find(ApiKeys.OFFSET_FETCH.id);
                assertThat(offsetFetchVersion).isNotNull();
                assertThat(offsetFetchVersion.maxVersion()).isLessThanOrEqualTo((short) 9);

                var deleteTopicsVersion = body.apiKeys().find(ApiKeys.DELETE_TOPICS.id);
                assertThat(deleteTopicsVersion).isNotNull();
                assertThat(deleteTopicsVersion.maxVersion()).isLessThanOrEqualTo((short) 5);
            }
        }

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.API_VERSIONS))
                .as("API_VERSIONS should be routed to default route")
                .hasSize(1);
    }
}
