/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.message.DescribeProducersResponseData.PartitionResponse;
import org.apache.kafka.common.message.DescribeProducersResponseData.TopicResponse;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

class DescribeProducersEnforcement extends ApiEnforcement<DescribeProducersRequestData, DescribeProducersResponseData> {
    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 0;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, DescribeProducersRequestData request, FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        if (request.topics() == null || request.topics().isEmpty()) {
            return context.forwardRequest(header, request);
        }
        else {
            List<Action> actions = request.topics().stream()
                    .map(topic -> new Action(TopicResource.READ, topic.name())).toList();
            return authorizationFilter.authorization(context, actions).thenCompose(result -> {
                var partitioned = result.partition(request.topics(), TopicResource.READ,
                        DescribeProducersRequestData.TopicRequest::name);
                List<DescribeProducersRequestData.TopicRequest> denied = partitioned.get(Decision.DENY);
                if (!denied.isEmpty()) {
                    Map<String, Integer> originalIndex = denied.stream()
                            .collect(Collectors.toMap(DescribeProducersRequestData.TopicRequest::name, t -> request.topics().indexOf(t)));
                    request.topics().removeAll(denied);
                    authorizationFilter.pushInflightState(header, (DescribeProducersResponseData producersResponseData) -> {
                        for (int i = denied.size() - 1; i >= 0; i--) {
                            DescribeProducersRequestData.TopicRequest topicRequest = denied.get(i);
                            TopicResponse topicResponse = new TopicResponse();
                            topicResponse.setName(topicRequest.name());
                            topicRequest.partitionIndexes().forEach(partitionIndex -> {
                                PartitionResponse response = new PartitionResponse();
                                response.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
                                response.setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message());
                                response.setPartitionIndex(partitionIndex);
                                topicResponse.partitions().add(response);
                            });
                            // re-insert items in the original order
                            Integer finalIndex = originalIndex.get(topicRequest.name());
                            int adjusted = finalIndex - i;
                            producersResponseData.topics().add(adjusted, topicResponse);
                        }
                        return producersResponseData;
                    });
                }
                return context.forwardRequest(header, request);
            });
        }
    }
}
