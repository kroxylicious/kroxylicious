/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

import static org.apache.kafka.common.requests.FetchResponse.partitionResponse;

/**
 * Initially no support for topic ids
 */
class FetchEnforcement extends ApiEnforcement<FetchRequestData, FetchResponseData> {

    // lowest version supported by proxy
    short minSupportedVersion() {
        return 4;
    }

    short maxSupportedVersion() {
        return 12;
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
                    var topicReadDecisions = authorization.partition(request.topics(),
                            TopicResource.READ,
                            FetchRequestData.FetchTopic::topic);
                    List<FetchRequestData.FetchTopic> allowedTopics = topicReadDecisions.getOrDefault(Decision.ALLOW, List.of());
                    if (allowedTopics.isEmpty()) {
                        // Shortcircuit if there's no allowed topics
                        FetchResponseData response = new FetchResponseData();
                        response.setErrorCode(Errors.NONE.code());
                        var fetchableTopicResponses = createDenyTopicResponses(topicReadDecisions);
                        response.setResponses(fetchableTopicResponses);
                        return context.requestFilterResultBuilder()
                                .shortCircuitResponse(response)
                                .completed();
                    }
                    else if (topicReadDecisions.getOrDefault(Decision.DENY, List.of()).isEmpty()) {
                        // Just forward if there's no denied topics
                        return context.forwardRequest(header, request);
                    }
                    else {
                        request.setTopics(allowedTopics);
                        var fetchableTopicResponses = createDenyTopicResponses(topicReadDecisions);
                        authorizationFilter.pushInflightState(header, (FetchResponseData response) -> {
                            response.responses().addAll(fetchableTopicResponses);
                            return response;
                        });
                        return context.forwardRequest(header, request);
                    }
                });
    }

    private static List<FetchResponseData.FetchableTopicResponse> createDenyTopicResponses(
                                                                                           Map<Decision, List<FetchRequestData.FetchTopic>> topicReadDecisions) {
        return topicReadDecisions.get(Decision.DENY)
                .stream().map(t -> new FetchResponseData.FetchableTopicResponse()
                        .setTopic(t.topic())
                        .setTopicId(t.topicId())
                        .setPartitions(t.partitions().stream().map(p -> partitionResponse(p.partition(), Errors.TOPIC_AUTHORIZATION_FAILED)).toList()))
                .toList();
    }

}
