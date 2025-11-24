/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.ArrayList;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

class OffsetDeleteEnforcement extends ApiEnforcement<OffsetDeleteRequestData, OffsetDeleteResponseData> {
    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 0;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   OffsetDeleteRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {

        var actions = request.topics().stream()
                .map(odrd -> new Action(TopicResource.READ, odrd.name()))
                .toList();
        return authorizationFilter.authorization(context, actions)
                .thenCompose(authorization -> {
                    var decisions = authorization.partition(request.topics(),
                            TopicResource.READ,
                            OffsetDeleteRequestData.OffsetDeleteRequestTopic::name);
                    if (decisions.get(Decision.ALLOW).isEmpty()) {
                        // Shortcircuit if there's no allowed topics
                        var creatableTopics = decisions.get(Decision.DENY).stream()
                                .map(this::topicAuthzFailed)
                                .toList();
                        var x = new OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection();
                        x.addAll(creatableTopics);
                        return context.requestFilterResultBuilder().shortCircuitResponse(
                                new ResponseHeaderData().setCorrelationId(header.correlationId()),
                                new OffsetDeleteResponseData().setTopics(x)).completed();
                    }
                    else if (decisions.get(Decision.DENY).isEmpty()) {
                        // Just forward if there's no denied topics
                        return context.forwardRequest(header, request);
                    }
                    else {
                        var topicCollection = new ArrayList<OffsetDeleteRequestData.OffsetDeleteRequestTopic>();
                        for (var topic : decisions.get(Decision.ALLOW)) {
                            topicCollection.add(topic.duplicate());
                        }
                        var coln = new OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection();
                        coln.addAll(topicCollection);
                        request.setTopics(coln);
                        var creatableTopicResults = decisions.get(Decision.DENY)
                                .stream().map(this::topicAuthzFailed)
                                .toList();
                        authorizationFilter.pushInflightState(header, (OffsetDeleteResponseData response) -> {
                            response.topics().addAll(creatableTopicResults);
                            return response;
                        });
                        return context.forwardRequest(header, request);
                    }
                });
    }

    private OffsetDeleteResponseData.OffsetDeleteResponseTopic topicAuthzFailed(OffsetDeleteRequestData.OffsetDeleteRequestTopic requestTopic) {
        var result = new OffsetDeleteResponseData.OffsetDeleteResponseTopic()
                .setName(requestTopic.name());
        result.partitions().addAll(requestTopic.partitions().stream().map(p -> {
            return new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
                    .setPartitionIndex(p.partitionIndex())
                    .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
        }).toList());
        return result;
    }
}
