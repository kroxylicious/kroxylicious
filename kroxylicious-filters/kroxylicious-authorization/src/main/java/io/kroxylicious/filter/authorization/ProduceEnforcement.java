/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

public class ProduceEnforcement extends ApiEnforcement<ProduceRequestData, ProduceResponseData> {
    @Override
    short minSupportedVersion() {
        return 3;
    }

    @Override
    short maxSupportedVersion() {
        return 12;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   ProduceRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        boolean requiresResponse = request.acks() != 0;
        var topicWriteActions = request.topicData().stream()
                .map(t -> new Action(TopicResource.WRITE, t.name()))
                .toList();

        return authorizationFilter.authorization(context, topicWriteActions).thenCompose(authorization -> {

            var topicWriteDecisions = authorization.partition(request.topicData(),
                    TopicResource.WRITE, ProduceRequestData.TopicProduceData::name);

            var allowedTopicWrites = topicWriteDecisions.get(Decision.ALLOW);
            if (allowedTopicWrites.isEmpty()) {
                if (requiresResponse) {
                    return context.requestFilterResultBuilder()
                            .errorResponse(header, request, Errors.TOPIC_AUTHORIZATION_FAILED.exception())
                            .completed();
                }
                else {
                    return context.requestFilterResultBuilder().drop().completed();
                }
            }

            var deniedTopicWrites = topicWriteDecisions.get(Decision.DENY);

            if (deniedTopicWrites.isEmpty()) {
                // nothing denied, forward whole request on
                return context.forwardRequest(header, request);
            }

            for (var topic : deniedTopicWrites) {
                request.topicData().remove(topic);
            }

            var topicProduceResponses = deniedTopicWrites.stream()
                    .map(topicProduceData -> new ProduceResponseData.TopicProduceResponse()
                            .setName(topicProduceData.name())
                            .setPartitionResponses(topicProduceData.partitionData().stream()
                                    .map(partitionProduceData -> new ProduceResponseData.PartitionProduceResponse()
                                            .setIndex(partitionProduceData.index())
                                            .setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message())
                                            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code()))
                                    .toList()))
                    .toList();

            if (requiresResponse) {
                authorizationFilter.pushInflightState(header, (ProduceResponseData response) -> {
                    response.responses().addAll(topicProduceResponses);
                    return response;
                });
            }
            return context.forwardRequest(header, request);
        });
    }

}
