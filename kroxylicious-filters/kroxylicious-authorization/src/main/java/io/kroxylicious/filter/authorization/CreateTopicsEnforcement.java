/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

class CreateTopicsEnforcement extends ApiEnforcement<CreateTopicsRequestData, CreateTopicsResponseData> {

    @Override
    short minSupportedVersion() {
        return 2;
    }

    @Override
    short maxSupportedVersion() {
        return 7;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                          CreateTopicsRequestData request,
                                                          FilterContext context,
                                                          AuthorizationFilter filter) {
        var topicReadActions = request.topics().stream()
                .map(ctrd -> new Action(TopicResource.CREATE, ctrd.name()))
                .toList();
        return filter.authorization(context, topicReadActions)
                .thenCompose(authorization -> {
                    var decisions = authorization.partition(request.topics(),
                            TopicResource.CREATE,
                            CreateTopicsRequestData.CreatableTopic::name);
                    if (decisions.get(Decision.ALLOW).isEmpty()) {
                        // Shortcircuit if there's no allowed topics
                        CreateTopicsResponseData.CreatableTopicResultCollection creatableTopics = new CreateTopicsResponseData.CreatableTopicResultCollection();
                        decisions.get(Decision.DENY).stream()
                                .map(ct -> topicAuthzFailed(header.requestApiVersion(), ct))
                                .forEach(creatableTopics::mustAdd);
                        return context.requestFilterResultBuilder().shortCircuitResponse(
                                new ResponseHeaderData().setCorrelationId(header.correlationId()),
                                new CreateTopicsResponseData().setTopics(creatableTopics)).completed();
                    }
                    else if (decisions.get(Decision.DENY).isEmpty()) {
                        // Just forward if there's no denied topics
                        return context.forwardRequest(header, request);
                    }
                    else {
                        var xx = new CreateTopicsRequestData.CreatableTopicCollection();
                        for (var yy : decisions.get(Decision.ALLOW)) {
                            xx.mustAdd(yy.duplicate());
                        }
                        request.setTopics(xx);
                        var creatableTopicResults = decisions.get(Decision.DENY)
                                .stream().map(t -> topicAuthzFailed(header.requestApiVersion(), t))
                                .toList();
                        filter.pushInflightState(header, (CreateTopicsResponseData response) -> {
                            response.topics().addAll(creatableTopicResults);
                            return response;
                        });
                        return context.forwardRequest(header, request);
                    }
                });
    }

    static CreateTopicsResponseData.CreatableTopicResult topicAuthzFailed(short apiVersion,
                                                                          CreateTopicsRequestData.CreatableTopic creatableTopic) {
        return new CreateTopicsResponseData.CreatableTopicResult()
                .setName(creatableTopic.name())
                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())
                .setErrorMessage(apiVersion >= 1 ? "Authorization failed." : null);
    }

    /**
     * Filter out any topic configs if the subject lacks DESCRIBE_CONFIGS
     * Append responses buffered when checking the request
     */
    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                            CreateTopicsResponseData response,
                                                            FilterContext context,
                                                            AuthorizationFilter filter) {

        List<Action> actions = response.topics().stream()
                .map(ctr -> new Action(TopicResource.DESCRIBE_CONFIGS, ctr.name()))
                .toList();
        return filter.authorization(context, actions)
                .thenCompose(authorization -> {

                    for (var creatableTopicResult : response.topics()) {
                        if (authorization.decision(TopicResource.DESCRIBE_CONFIGS, creatableTopicResult.name()) == Decision.DENY) {
                            creatableTopicResult.setConfigs(List.of());
                            creatableTopicResult.setReplicationFactor((short) -1);
                            creatableTopicResult.setNumPartitions(-1);
                            creatableTopicResult.setTopicConfigErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
                        }
                    }
                    return context.forwardResponse(header, filter.popAndApplyInflightState(header, response));
                });
    }

}
