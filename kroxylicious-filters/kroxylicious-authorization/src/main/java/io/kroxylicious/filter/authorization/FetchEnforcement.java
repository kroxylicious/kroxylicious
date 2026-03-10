/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import io.kroxylicious.proxy.filter.metadata.TopicNameMappingException;

import static org.apache.kafka.common.requests.FetchResponse.partitionResponse;

class FetchEnforcement extends ApiEnforcement<FetchRequestData, FetchResponseData> {

    // lowest version supported by proxy
    short minSupportedVersion() {
        return 4;
    }

    short maxSupportedVersion() {
        return 18;
    }

    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   FetchRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        List<Uuid> topicIds = request.topics().stream()
                .map(FetchRequestData.FetchTopic::topicId)
                .filter(uuid -> !Uuid.ZERO_UUID.equals(uuid))
                .toList();
        return context.topicNames(topicIds).thenCompose(topicNameMapping -> {
            if (topicNameMapping.allFailures()) {
                return allTopicNameLookupsFailed(request, context, topicNameMapping);
            }
            List<FetchRequestData.FetchTopic> failedTopicIdLookups = request.topics().stream()
                    .filter(fetchTopic -> topicNameMapping.failures().containsKey(fetchTopic.topicId()))
                    .toList();
            request.topics().removeAll(failedTopicIdLookups);
            List<FetchResponseData.FetchableTopicResponse> failedTopicIdLookupResponses = failedTopicIdLookups.stream()
                    .map(fetchTopic -> toTopicIdLookupFailureResponse(topicNameMapping, fetchTopic))
                    .toList();
            var topicReadActions = request.topics().stream()
                    .map(t -> new Action(TopicResource.READ, getTopicName(t, topicNameMapping)))
                    .toList();
            return authorizationFilter.authorization(context, topicReadActions)
                    .thenCompose(authorization -> {
                        var topicReadDecisions = authorization.partition(request.topics(),
                                TopicResource.READ,
                                fetchTopic -> getTopicName(fetchTopic, topicNameMapping));
                        List<FetchRequestData.FetchTopic> allowedTopics = topicReadDecisions.getOrDefault(Decision.ALLOW, List.of());
                        if (allowedTopics.isEmpty()) {
                            // Shortcircuit if there are no allowed topics
                            FetchResponseData response = new FetchResponseData();
                            response.setErrorCode(Errors.NONE.code());
                            var fetchableTopicResponses = createFailedTopicResponses(topicReadDecisions, failedTopicIdLookupResponses);
                            response.setResponses(fetchableTopicResponses);
                            return context.requestFilterResultBuilder()
                                    .shortCircuitResponse(response)
                                    .completed();
                        }
                        else if (topicReadDecisions.getOrDefault(Decision.DENY, List.of()).isEmpty() && failedTopicIdLookupResponses.isEmpty()) {
                            // Just forward if there are no denied topics and no failed topic lookup responses
                            return context.forwardRequest(header, request);
                        }
                        else {
                            request.setTopics(allowedTopics);
                            var fetchableTopicResponses = createFailedTopicResponses(topicReadDecisions, failedTopicIdLookupResponses);
                            authorizationFilter.pushInflightState(header, (FetchResponseData response) -> {
                                response.responses().addAll(fetchableTopicResponses);
                                return response;
                            });
                            return context.forwardRequest(header, request);
                        }
                    });
        });
    }

    private static String getTopicName(FetchRequestData.FetchTopic t, TopicNameMapping topicNameMapping) {
        return Optional.ofNullable(t.topic())
                .filter(s -> !s.isEmpty())
                .or(() -> Optional.ofNullable(topicNameMapping.topicNames().get(t.topicId())))
                // state should be impossible as we are operating on a FetchTopic that was not filtered due to missing topicName
                .orElseThrow(() -> new IllegalStateException("Could not determine name for FetchTopic{topic='" + t.topic() + "', topicId:'" + t.topicId() + "}"));
    }

    private static CompletionStage<RequestFilterResult> allTopicNameLookupsFailed(FetchRequestData request, FilterContext context,
                                                                                  TopicNameMapping topicNameMapping) {
        FetchResponseData response = new FetchResponseData();
        response.setErrorCode(Errors.NONE.code());
        var fetchableTopicResponses = request.topics().stream()
                .map(t -> toTopicIdLookupFailureResponse(topicNameMapping, t))
                .toList();
        response.setResponses(fetchableTopicResponses);
        return context.requestFilterResultBuilder()
                .shortCircuitResponse(response)
                .completed();
    }

    private static FetchResponseData.FetchableTopicResponse toTopicIdLookupFailureResponse(TopicNameMapping topicNameMapping, FetchRequestData.FetchTopic t) {
        return new FetchResponseData.FetchableTopicResponse()
                .setTopic(t.topic())
                .setTopicId(t.topicId())
                .setPartitions(t.partitions().stream().map(p -> partitionResponse(p.partition(),
                        Optional.ofNullable(topicNameMapping.failures().get(t.topicId())).map(TopicNameMappingException::getError)
                                .orElse(Errors.NETWORK_EXCEPTION)))
                        .toList());
    }

    private static List<FetchResponseData.FetchableTopicResponse> createFailedTopicResponses(
                                                                                             Map<Decision, List<FetchRequestData.FetchTopic>> topicReadDecisions,
                                                                                             List<FetchResponseData.FetchableTopicResponse> failedTopicIdLookupResponses) {
        return Stream.concat(deniedTopicResponses(topicReadDecisions), failedTopicIdLookupResponses.stream())
                .toList();
    }

    private static Stream<FetchResponseData.FetchableTopicResponse> deniedTopicResponses(Map<Decision, List<FetchRequestData.FetchTopic>> topicReadDecisions) {
        return topicReadDecisions.get(Decision.DENY)
                .stream().map(t -> new FetchResponseData.FetchableTopicResponse()
                        .setTopic(t.topic())
                        .setTopicId(t.topicId())
                        .setPartitions(t.partitions().stream().map(p -> partitionResponse(p.partition(), Errors.TOPIC_AUTHORIZATION_FAILED)).toList()));
    }

}
