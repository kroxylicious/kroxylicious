/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.ArrayList;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

class OffsetCommitEnforcement extends ApiEnforcement<OffsetCommitRequestData, OffsetCommitResponseData> {

    public static final int LAST_VERSION_WITH_TOPIC_NAMES = 9;

    @Override
    short minSupportedVersion() {
        return 2;
    }

    @Override
    short maxSupportedVersion() {
        return LAST_VERSION_WITH_TOPIC_NAMES; // doesn't currently support topicids
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   OffsetCommitRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        var actions = request.topics().stream()
                .map(ocrd -> new Action(TopicResource.READ, ocrd.name()))
                .toList();
        return authorizationFilter.authorization(context, actions)
                .thenCompose(authorization -> {
                    var decisions = authorization.partition(request.topics(),
                            TopicResource.READ,
                            OffsetCommitRequestData.OffsetCommitRequestTopic::name);
                    if (decisions.get(Decision.ALLOW).isEmpty()) {
                        // Shortcircuit if there are no allowed topics
                        var creatableTopics = decisions.get(Decision.DENY).stream()
                                .map(this::topicAuthzFailed)
                                .toList();
                        return context.requestFilterResultBuilder().shortCircuitResponse(
                                new ResponseHeaderData().setCorrelationId(header.correlationId()),
                                new OffsetCommitResponseData().setTopics(creatableTopics)).completed();
                    }
                    else if (decisions.get(Decision.DENY).isEmpty()) {
                        // Just forward if there are no denied topics
                        return context.forwardRequest(header, request);
                    }
                    else {
                        var topicCollection = new ArrayList<OffsetCommitRequestData.OffsetCommitRequestTopic>();
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

    private OffsetCommitResponseData.OffsetCommitResponseTopic topicAuthzFailed(OffsetCommitRequestData.OffsetCommitRequestTopic offsetCommitRequestTopic) {
        return new OffsetCommitResponseData.OffsetCommitResponseTopic()
                .setName(offsetCommitRequestTopic.name())
                .setPartitions(offsetCommitRequestTopic.partitions().stream()
                        .map(p -> new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(p.partitionIndex())
                                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code()))
                        .toList());
    }

}
