/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

class CreatePartitionsEnforcement extends ApiEnforcement<CreatePartitionsRequestData, CreatePartitionsResponseData> {

    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 3;
    }

    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   CreatePartitionsRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        var actions = request.topics().stream()
                .map(cpd -> new Action(TopicResource.ALTER, cpd.name()))
                .toList();
        return authorizationFilter.authorization(context, actions)
                .thenCompose(authorization -> {
                    var decisions = authorization.partition(request.topics(),
                            TopicResource.ALTER,
                            CreatePartitionsRequestData.CreatePartitionsTopic::name);
                    if (decisions.get(Decision.ALLOW).isEmpty()) {
                        // Shortcircuit if there are no allowed topics
                        var creatableTopics = decisions.get(Decision.DENY).stream()
                                .map(CreatePartitionsEnforcement::topicAuthzFailed)
                                .toList();
                        return context.requestFilterResultBuilder().shortCircuitResponse(
                                new ResponseHeaderData().setCorrelationId(header.correlationId()),
                                new CreatePartitionsResponseData().setResults(creatableTopics)).completed();
                    }
                    else if (decisions.get(Decision.DENY).isEmpty()) {
                        // Just forward if there are no denied topics
                        return context.forwardRequest(header, request);
                    }
                    else {
                        var topicCollection = new CreatePartitionsRequestData.CreatePartitionsTopicCollection();
                        for (var topic : decisions.get(Decision.ALLOW)) {
                            topicCollection.mustAdd(topic.duplicate());
                        }
                        request.setTopics(topicCollection);
                        var creatableTopicResults = decisions.get(Decision.DENY)
                                .stream().map(CreatePartitionsEnforcement::topicAuthzFailed)
                                .toList();
                        authorizationFilter.pushInflightState(header, (CreatePartitionsResponseData response) -> {
                            response.results().addAll(creatableTopicResults);
                            return response;
                        });
                        return context.forwardRequest(header, request);
                    }
                });
    }

    static CreatePartitionsResponseData.CreatePartitionsTopicResult topicAuthzFailed(CreatePartitionsRequestData.CreatePartitionsTopic t) {
        return new CreatePartitionsResponseData.CreatePartitionsTopicResult()
                .setName(t.name())
                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }
}
