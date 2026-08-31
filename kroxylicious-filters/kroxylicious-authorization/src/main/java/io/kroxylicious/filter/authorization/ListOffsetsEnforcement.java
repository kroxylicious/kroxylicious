/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.ListOffsetsRequestData;
import io.kroxylicious.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import io.kroxylicious.kafka.common.message.ListOffsetsResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

class ListOffsetsEnforcement extends ApiEnforcement<ListOffsetsRequestData, ListOffsetsResponseData> {

    @Override
    short minSupportedVersion() {
        return 1;
    }

    @Override
    short maxSupportedVersion() {
        return 11;
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
        return listOffsetsTopics.stream().map(listOffsetsTopic -> {
            List<ListOffsetsResponseData.ListOffsetsPartitionResponse> partitionResponses = listOffsetsTopic.partitions().stream()
                    .map(listOffsetsPartition -> new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                            .setPartitionIndex(listOffsetsPartition.partitionIndex())
                            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())
                            .setTimestamp(-1L) // Unknown timestamp
                            .setOffset(-1L)) // Unknown offset
                    .toList();
            ListOffsetsResponseData.ListOffsetsTopicResponse responseTopic = new ListOffsetsResponseData.ListOffsetsTopicResponse();
            responseTopic.setName(listOffsetsTopic.name());
            responseTopic.setPartitions(partitionResponses);
            return responseTopic;
        }).toList();
    }
}
