/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

public class DeleteTopicsEnforcement extends ApiEnforcement<DeleteTopicsRequestData, DeleteTopicsResponseData> {

    @Override
    short minSupportedVersion() {
        return 1;
    }

    @Override
    short maxSupportedVersion() {
        // DELETE_TOPICS: v6 allows topic ids: It's better to force the client to use v5 than fail a v6 later on
        // when the client goes ahead and uses topic ids
        return 5;
    }

    /**
     * Buffer responses for topics where the subject lacks DELETE
     */
    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   DeleteTopicsRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        short apiVersion = header.requestApiVersion();
        boolean useStates = apiVersion >= 6;
        List<Action> actions;
        TopicResource operation = TopicResource.DELETE;
        if (useStates) {
            actions = operation.actionsOf(request.topics().stream()
                    .map(t -> {
                        if (t.name() != null) {
                            return t.name();
                        }
                        else {
                            throw authorizationFilter.topicIdsNotSupported();
                        }
                    }));
        }
        else {
            actions = operation.actionsOf(request.topicNames().stream());
        }
        return authorizationFilter.authorization(context, actions)
                .thenCompose(authorization -> {
                    if (useStates) {
                        var decisions = authorization.partition(request.topics(), operation, DeleteTopicsRequestData.DeleteTopicState::name);
                        if (decisions.get(Decision.ALLOW).isEmpty()) {
                            // Shortcircuit if there's no allowed actions
                            DeleteTopicsResponseData.DeletableTopicResultCollection v = new DeleteTopicsResponseData.DeletableTopicResultCollection();
                            request.topics().stream()
                                    .map(topicState -> topicAuthzFailed(apiVersion, topicState))
                                    .forEach(v::mustAdd);
                            return context.requestFilterResultBuilder()
                                    .shortCircuitResponse(
                                            new DeleteTopicsResponseData()
                                                    .setResponses(v))
                                    .completed();
                        }
                        else if (decisions.get(Decision.DENY).isEmpty()) {
                            // Just forward if there's no denied actions
                            return context.forwardRequest(header, request);
                        }
                        else {
                            request.setTopics(request.topics().stream()
                                    .filter(topicState -> authorization.decision(operation, topicState.name()) == Decision.ALLOW)
                                    .toList());

                            var list = decisions.get(Decision.DENY)
                                    .stream().map(t -> topicAuthzFailed(apiVersion, t))
                                    .toList();
                            authorizationFilter.pushInflightState(header, (DeleteTopicsResponseData response) -> {
                                response.responses().addAll(list);
                                return response;
                            });
                            return context.forwardRequest(header, request);
                        }
                    }
                    else { // using topic names
                        var decisions = authorization.partition(request.topicNames(), operation, Function.identity());
                        if (decisions.get(Decision.ALLOW).isEmpty()) {
                            // Shortcircuit if there's no allowed actions
                            DeleteTopicsResponseData.DeletableTopicResultCollection v = new DeleteTopicsResponseData.DeletableTopicResultCollection();
                            request.topicNames().stream()
                                    .map(topicName -> topicAuthzFailed(apiVersion, topicName))
                                    .forEach(v::mustAdd);
                            return context.requestFilterResultBuilder()
                                    .shortCircuitResponse(
                                            new DeleteTopicsResponseData()
                                                    .setResponses(v))
                                    .completed();
                        }
                        else if (decisions.get(Decision.DENY).isEmpty()) {
                            // Just forward if there's no denied actions
                            return context.forwardRequest(header, request);
                        }
                        else {
                            request.setTopicNames(request.topicNames().stream()
                                    .filter(tn -> authorization.decision(operation, tn) == Decision.ALLOW)
                                    .toList());

                            var list = decisions.get(Decision.DENY)
                                    .stream().map(t -> topicAuthzFailed(apiVersion, t))
                                    .toList();
                            authorizationFilter.pushInflightState(header, (DeleteTopicsResponseData response) -> {
                                response.responses().addAll(list);
                                return response;
                            });
                            return context.forwardRequest(header, request);
                        }
                    }
                });
    }

    static DeleteTopicsResponseData.DeletableTopicResult topicAuthzFailed(short apiVersion,
                                                                          DeleteTopicsRequestData.DeleteTopicState state) {

        if (apiVersion < 6) {
            throw new IllegalStateException();
        }
        return topicAuthzFailed(apiVersion, new DeleteTopicsResponseData.DeletableTopicResult())
                .setTopicId(state.topicId())
                .setName(state.name());
    }

    static DeleteTopicsResponseData.DeletableTopicResult topicAuthzFailed(short apiVersion,
                                                                          DeleteTopicsResponseData.DeletableTopicResult topicResult) {
        return topicResult
                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())
                .setErrorMessage(apiVersion >= 5 ? Errors.TOPIC_AUTHORIZATION_FAILED.message() : null);
    }

    static DeleteTopicsResponseData.DeletableTopicResult topicAuthzFailed(short apiVersion,
                                                                          String topicName) {
        if (apiVersion >= 6) {
            throw new IllegalStateException();
        }
        return topicAuthzFailed(apiVersion, new DeleteTopicsResponseData.DeletableTopicResult())
                .setName(topicName);
    }
}
