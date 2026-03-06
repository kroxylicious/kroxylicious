/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;

class OffsetCommitEnforcement extends ApiEnforcement<OffsetCommitRequestData, OffsetCommitResponseData> {

    @Override
    short minSupportedVersion() {
        return 2;
    }

    @Override
    short maxSupportedVersion() {
        return 10;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   OffsetCommitRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        List<Uuid> topicIds = request.topics().stream().map(OffsetCommitRequestTopic::topicId)
                .filter(uuid -> !Uuid.ZERO_UUID.equals(uuid)).toList();
        return context.topicNames(topicIds).thenCompose(topicNameMapping -> onRequest(header, request, context, authorizationFilter, topicNameMapping));
    }

    private CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                           OffsetCommitRequestData request,
                                                           FilterContext context,
                                                           AuthorizationFilter authorizationFilter,
                                                           TopicNameMapping topicNameMapping) {
        if (topicNameMapping.anyFailures()) {
            return context.requestFilterResultBuilder().errorResponse(header, request, Errors.UNKNOWN_TOPIC_ID.exception()).completed();
        }

        var actions = request.topics().stream()
                .map(ocrd -> new Action(TopicResource.READ, mustGetTopicName(topicNameMapping, ocrd)))
                .toList();
        return authorizationFilter.authorization(context, actions)
                .thenCompose(authorization -> {
                    var decisions = authorization.partition(request.topics(),
                            TopicResource.READ,
                            requestTopic -> mustGetTopicName(topicNameMapping, requestTopic));
                    if (decisions.get(Decision.ALLOW).isEmpty()) {
                        // Shortcircuit if there are no allowed topics
                        var creatableTopics = decisions.get(Decision.DENY).stream()
                                .map(this::topicAuthzFailed)
                                .toList();
                        return context.requestFilterResultBuilder().shortCircuitResponse(
                                new OffsetCommitResponseData().setTopics(creatableTopics)).completed();
                    }
                    else if (decisions.get(Decision.DENY).isEmpty()) {
                        // Just forward if there are no denied topics
                        return context.forwardRequest(header, request);
                    }
                    else {
                        var topicCollection = new ArrayList<OffsetCommitRequestTopic>();
                        for (var topic : decisions.get(Decision.ALLOW)) {
                            topicCollection.add(topic.duplicate());
                        }
                        request.setTopics(topicCollection);
                        var creatableTopicResults = decisions.get(Decision.DENY)
                                .stream().map(this::topicAuthzFailed)
                                .toList();
                        authorizationFilter.pushInflightState(header, (OffsetCommitResponseData response) -> {
                            response.topics().addAll(creatableTopicResults);
                            return response;
                        });
                        return context.forwardRequest(header, request);
                    }
                });
    }

    /**
     * get the name of the topic, mapping from topicId to name if required, throwing an exception if it is unknown
     * @param topicNameMapping mapping
     * @param requestTopic topic
     * @return the topic name
     * @throws IllegalStateException if the topic name is not available
     */
    private static String mustGetTopicName(TopicNameMapping topicNameMapping, OffsetCommitRequestTopic requestTopic) {
        if (requestTopic.name() != null && !requestTopic.name().isEmpty()) {
            return requestTopic.name();
        }
        else {
            String topicName = topicNameMapping.topicNames().get(requestTopic.topicId());
            if (topicName == null) {
                throw new IllegalStateException("no name discovered for OffsetCommitRequestTopic with name: " + requestTopic.name() + " and topicId: "
                        + requestTopic.topicId());
            }
            else {
                return topicName;
            }
        }
    }

    private OffsetCommitResponseData.OffsetCommitResponseTopic topicAuthzFailed(OffsetCommitRequestTopic requestTopic) {
        return new OffsetCommitResponseData.OffsetCommitResponseTopic()
                .setName(requestTopic.name())
                .setTopicId(requestTopic.topicId())
                .setPartitions(requestTopic.partitions().stream()
                        .map(p -> new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(p.partitionIndex())
                                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code()))
                        .toList());
    }

}
