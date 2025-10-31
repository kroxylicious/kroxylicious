/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

class FetchEnforcement extends ApiEnforcement<FetchRequestData, FetchResponseData> {

    short minSupportedVersion() {
        return 0;
    }

    short maxSupportedVersion() {
        return 13;
    }

    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   FetchRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        var topicReadActions = request.topics().stream()
                .map(t -> new Action(TopicResource.READ, t.topic()))
                .toList();
        return authorizationFilter.authorization(context, topicReadActions)
                .thenCompose(authorization -> {
                    var topicReadDecisions = request.topics().stream()
                            .collect(Collectors.groupingBy(t -> authorization.decision(TopicResource.READ, t.topic())));
                    if (topicReadDecisions.get(Decision.ALLOW).isEmpty()) {
                        // Shortcircuit if there's no allowed topics
                        return context.requestFilterResultBuilder()
                                .errorResponse(header, request, Errors.TOPIC_AUTHORIZATION_FAILED.exception())
                                .completed();
                    }
                    else if (topicReadDecisions.get(Decision.DENY).isEmpty()) {
                        // Just forward if there's no denied topics
                        return context.forwardRequest(header, request);
                    }
                    else {
                        request.setTopics(topicReadDecisions.get(Decision.ALLOW));
                        var fetchableTopicResponses = topicReadDecisions.get(Decision.DENY)
                                .stream().map(t -> new FetchResponseData.FetchableTopicResponse()
                                        .setTopic(t.topic())
                                        .setTopicId(t.topicId())
                                        .setPartitions(t.partitions().stream().map(p -> new FetchResponseData.PartitionData()
                                                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())).toList()))
                                .toList();
                        authorizationFilter.pushInflightState(header, (FetchResponseData response) -> {
                            response.responses().addAll(fetchableTopicResponses);
                            return response;
                        });
                        return context.forwardRequest(header, request);
                    }
                });
    }

}
