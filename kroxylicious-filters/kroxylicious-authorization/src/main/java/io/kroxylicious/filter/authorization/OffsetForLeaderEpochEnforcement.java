/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET;

public class OffsetForLeaderEpochEnforcement extends ApiEnforcement<OffsetForLeaderEpochRequestData, OffsetForLeaderEpochResponseData> {
    // lowest supported by proxy
    @Override
    short minSupportedVersion() {
        return 2;
    }

    @Override
    short maxSupportedVersion() {
        return 4;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   OffsetForLeaderEpochRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        List<Action> actions = request.topics().stream().map(OffsetForLeaderEpochRequestData.OffsetForLeaderTopic::topic).map(s -> new Action(TopicResource.DESCRIBE, s))
                .toList();
        CompletionStage<AuthorizeResult> authorization = authorizationFilter.authorization(context, actions);
        return authorization.thenCompose(result -> {
            Map<Decision, List<OffsetForLeaderEpochRequestData.OffsetForLeaderTopic>> partitioned = result.partition(request.topics(), TopicResource.DESCRIBE,
                    OffsetForLeaderEpochRequestData.OffsetForLeaderTopic::topic);
            if (partitioned.get(Decision.DENY).isEmpty()) {
                return context.forwardRequest(header, request);
            }
            else {
                List<OffsetForLeaderEpochRequestData.OffsetForLeaderTopic> toRemove = partitioned.get(Decision.DENY);
                List<OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult> removed = toRemove.stream().map(offsetForLeaderTopic -> {
                    OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult responseTopic = new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult();
                    responseTopic.setTopic(offsetForLeaderTopic.topic());
                    List<OffsetForLeaderEpochResponseData.EpochEndOffset> partitionErrors = offsetForLeaderTopic.partitions().stream()
                            .map(offsetForLeaderPartition -> new OffsetForLeaderEpochResponseData.EpochEndOffset().setPartition(offsetForLeaderPartition.partition())
                                    .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())
                                    .setLeaderEpoch(UNDEFINED_EPOCH)
                                    .setEndOffset(UNDEFINED_EPOCH_OFFSET))
                            .toList();
                    responseTopic.setPartitions(partitionErrors);
                    return responseTopic;
                }).toList();
                if (partitioned.get(Decision.ALLOW).isEmpty()) {
                    // short circuit, no topics in request allowed
                    OffsetForLeaderEpochResponseData message = new OffsetForLeaderEpochResponseData();
                    message.topics().addAll(removed);
                    return context.requestFilterResultBuilder().shortCircuitResponse(message).completed();
                }
                else {
                    request.topics().removeAll(toRemove);
                    authorizationFilter.pushInflightState(header, (OffsetForLeaderEpochResponseData response) -> {
                        response.topics().addAll(removed);
                        return response;
                    });
                    return context.forwardRequest(header, request);
                }
            }
        });
    }
}
