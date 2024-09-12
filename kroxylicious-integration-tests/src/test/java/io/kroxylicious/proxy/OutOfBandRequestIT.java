/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.proxy.filter.OutOfBandSendFilterFactory;
import io.kroxylicious.proxy.filter.RequestResponseMarkingFilter;
import io.kroxylicious.proxy.filter.RequestResponseMarkingFilterFactory;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.ResponsePayload;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.test.tester.KroxyliciousConfigUtils;
import io.kroxylicious.test.tester.KroxyliciousTesters;
import io.kroxylicious.test.tester.MockServerKroxyliciousTester;

import static io.kroxylicious.UnknownTaggedFields.unknownTaggedFieldsToStrings;
import static io.kroxylicious.proxy.filter.RequestResponseMarkingFilter.FILTER_NAME_TAG;
import static org.apache.kafka.common.protocol.ApiKeys.CREATE_TOPICS;
import static org.apache.kafka.common.protocol.ApiKeys.DESCRIBE_CLUSTER;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Given filter chain A -> B -> C. If B sends an out-of-band request X, then:
 * <ol>
 *     <li>C can Filter and mutate X before it is sent to the Cluster</li>
 *     <li>C can Filter and mutate the Cluster's response to X before the CompletionStage is completed</li>
 *     <li>The CompletionStage will be completed when the Cluster's response to X flows to B. This is the end of the flow.</li>
 *     <li>A can not filter X or the response to X, the flow was terminated.</li>
 * </ol>
 */
@ExtendWith(NettyLeakDetectorExtension.class)
public class OutOfBandRequestIT {

    @Test
    void testOutOfBandMessageInterceptedByUpstreamFilters() {
        // this filter should not intercept the out-of-band request or response, it will not either message with its name
        FilterDefinition downstreamFilter = addAddUnknownTaggedFieldToMessagesWithApiKey("downstreamOfOutOfBandFilter", CREATE_TOPICS);
        FilterDefinition outOfBandSender = outOfBandSender(CREATE_TOPICS, FILTER_NAME_TAG);
        // this filter should intercept the out-of-band request and response, it will tag both messages with its name
        FilterDefinition upstreamFilter = addAddUnknownTaggedFieldToMessagesWithApiKey("upstreamOfOutOfBandFilter", CREATE_TOPICS);
        try (var tester = createMockTesterWithFilters(downstreamFilter, outOfBandSender, upstreamFilter);
                var client = tester.simpleTestClient()) {
            givenMockReturnsArbitraryCreateTopicResponse(tester);
            givenMockReturnsArbitraryDescribeClusterResponse(tester);
            DescribeClusterResponseData responseData = whenDescribeCluster(client);
            thenResponseContainsTagsAugmentedInByUpstreamFilterOnly(responseData);
            andMessageFromOutOfBandRequestToMockHadTagAddedByUpstreamFilterOnly(tester);
            tester.assertAllMockInteractionsInvoked();
        }
    }

    private static void givenMockReturnsArbitraryDescribeClusterResponse(MockServerKroxyliciousTester tester) {
        DescribeClusterResponseData message = new DescribeClusterResponseData();
        message.setErrorMessage("arbitrary");
        message.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        tester.addMockResponseForApiKey(new ResponsePayload(DESCRIBE_CLUSTER, DESCRIBE_CLUSTER.latestVersion(), message));
    }

    private static MockServerKroxyliciousTester createMockTesterWithFilters(FilterDefinition... definitions) {
        List<FilterDefinition> filterDefs = Arrays.stream(definitions).toList();
        return KroxyliciousTesters.mockKafkaKroxyliciousTester(s -> KroxyliciousConfigUtils.proxy(s).addAllToFilters(filterDefs));
    }

    private static FilterDefinition outOfBandSender(ApiKeys apiKeyToSend, int tagToCollect) {
        return new FilterDefinitionBuilder(OutOfBandSendFilterFactory.class.getName()).withConfig(Map.of("apiKeyToSend", apiKeyToSend, "tagToCollect", tagToCollect))
                                                                                      .build();
    }

    private static FilterDefinition addAddUnknownTaggedFieldToMessagesWithApiKey(String name, ApiKeys apiKeys) {
        return new FilterDefinitionBuilder(RequestResponseMarkingFilterFactory.class.getName()).withConfig("name", name, "keysToMark", Set.of(apiKeys)).build();
    }

    private static void andMessageFromOutOfBandRequestToMockHadTagAddedByUpstreamFilterOnly(MockServerKroxyliciousTester tester) {
        Request request = tester.getOnlyRequestForApiKey(CREATE_TOPICS);
        String tags = unknownTaggedFieldsToStrings(request.message(), FILTER_NAME_TAG)
                                                                                      .collect(Collectors.joining(","));
        assertEquals(RequestResponseMarkingFilter.class.getSimpleName() + "-upstreamOfOutOfBandFilter-request", tags);
    }

    private static void thenResponseContainsTagsAugmentedInByUpstreamFilterOnly(DescribeClusterResponseData responseData) {
        assertEquals(
                "filterNameTaggedFieldsFromOutOfBandResponse: "
                     + RequestResponseMarkingFilter.class.getSimpleName()
                     + "-upstreamOfOutOfBandFilter-response",
                responseData.errorMessage()
        );
    }

    private static DescribeClusterResponseData whenDescribeCluster(KafkaClient client) {
        Response response = client.getSync(
                new Request(DESCRIBE_CLUSTER, DESCRIBE_CLUSTER.latestVersion(), "client", new DescribeClusterRequestData())
        );
        return (DescribeClusterResponseData) response.payload().message();
    }

    private static void givenMockReturnsArbitraryCreateTopicResponse(MockServerKroxyliciousTester tester) {
        CreateTopicsResponseData message = new CreateTopicsResponseData();
        CreateTopicsResponseData.CreatableTopicResult topic = new CreateTopicsResponseData.CreatableTopicResult();
        topic.setName("mockTopic");
        topic.setReplicationFactor((short) 3);
        topic.setNumPartitions(3);
        message.topics().add(topic);
        tester.addMockResponseForApiKey(new ResponsePayload(CREATE_TOPICS, CREATE_TOPICS.latestVersion(), message));
    }
}
