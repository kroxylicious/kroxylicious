/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ListOffsetsResponse;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

class ListOffsetsEnforcement extends ApiEnforcement<ListOffsetsRequestData, ListOffsetsResponseData> {

    short minSupportedVersion() {
        return 1;
    }

    short maxSupportedVersion() {
        return 10;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   ListOffsetsRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        List<Action> actions = request.topics().stream().map(ListOffsetsTopic::name)
                .map(s -> new Action(TopicResource.DESCRIBE, s)).toList();
        return authorizationFilter.authorization(context, actions)
                .thenCompose(authorization -> {
                    Map<Decision, List<ListOffsetsTopic>> topicDescribeDecisions = authorization.partition(request.topics(),
                            TopicResource.DESCRIBE,
                            ListOffsetsTopic::name);
                    List<ListOffsetsTopic> allowedTopics = topicDescribeDecisions.getOrDefault(Decision.ALLOW, List.of());
                    if (allowedTopics.isEmpty()) {
                        // Shortcircuit if there are no allowed topics
                        ListOffsetsResponseData response = new ListOffsetsResponseData();
                        var topics = createDenyTopicResponses(topicDescribeDecisions);
                        response.setTopics(topics);
                        return context.requestFilterResultBuilder()
                                .shortCircuitResponse(response)
                                .completed();
                    }
                    else if (topicDescribeDecisions.getOrDefault(Decision.DENY, List.of()).isEmpty()) {
                        // Just forward if there are no denied topics
                        return context.forwardRequest(header, request);
                    }
                    else {
                        var topics = createDenyTopicResponses(topicDescribeDecisions);
                        request.setTopics(allowedTopics);
                        authorizationFilter.pushInflightState(header,
                                (ListOffsetsResponseData response) -> {
                                    response.topics().addAll(topics);
                                    return response;
                                });
                        return context.forwardRequest(header, request);
                    }
                });
    }

    private List<ListOffsetsResponseData.ListOffsetsTopicResponse> createDenyTopicResponses(Map<Decision, List<ListOffsetsTopic>> topicDescribeDecisions) {
        List<ListOffsetsTopic> listOffsetsTopics = topicDescribeDecisions.get(Decision.DENY);
        return listOffsetsTopics.stream().map(t -> {
            List<ListOffsetsResponseData.ListOffsetsPartitionResponse> partitionResponses = t.partitions().stream()
                    .map(listOffsetsPartition -> new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                            .setPartitionIndex(listOffsetsPartition.partitionIndex())
                            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())
                            .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                            .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET))
                    .toList();
            ListOffsetsResponseData.ListOffsetsTopicResponse responseTopic = new ListOffsetsResponseData.ListOffsetsTopicResponse();
            responseTopic.setName(t.name());
            responseTopic.setPartitions(partitionResponses);
            return responseTopic;
        }).toList();
    }
}
