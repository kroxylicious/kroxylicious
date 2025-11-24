/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import io.kroxylicious.proxy.filter.metadata.TopicNameMappingException;

class DeleteTopicsEnforcement extends ApiEnforcement<DeleteTopicsRequestData, DeleteTopicsResponseData> {

    @Override
    short minSupportedVersion() {
        return 1;
    }

    @Override
    short maxSupportedVersion() {
        // DELETE_TOPICS: v6 allows topic ids: It's better to force the client to use v5 than fail a v6 later on
        // when the client goes ahead and uses topic ids
        return 6;
    }

    /**
     * Buffer responses for topics where the subject lacks DELETE
     */
    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   DeleteTopicsRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        TopicResource operation = TopicResource.DELETE;
        short apiVersion = header.requestApiVersion();
        var mappingStage = context.topicNames(
                request.topics() == null ? List.of()
                        : request.topics().stream()
                                .filter(t -> t.name() == null)
                                .map(DeleteTopicsRequestData.DeleteTopicState::topicId)
                                .toList());

        return mappingStage.thenCompose(mapping -> {

            Map<Uuid, String> topicIdToName = mapping.topicNames();
            boolean useStates = apiVersion >= 6;
            List<Action> actions;
            if (!useStates) {
                actions = request.topicNames().stream()
                        .map(topicName -> new Action(operation, topicName))
                        .toList();
            }
            else {
                actions = request.topics().stream()
                        .map(state -> topicName(state, topicIdToName))
                        .filter(Objects::nonNull)
                        .map(topicName -> new Action(operation, topicName))
                        .toList();
            }

            return authorizationFilter.authorization(context, actions).thenCompose(authorization -> {
                if (useStates) {
                    var errorPartitionedStates = request.topics().stream()
                            .collect(Collectors.partitioningBy(state -> state.name() != null || !mapping.failures().containsKey(state.topicId())));
                    var okStates = errorPartitionedStates.get(true);
                    var errorStates = errorPartitionedStates.get(false);
                    Map<Decision, List<DeleteTopicsRequestData.DeleteTopicState>> decisions = authorization.partition(
                            okStates,
                            operation,
                            state -> topicName(state, topicIdToName));
                    if (decisions.get(Decision.ALLOW).isEmpty()) {
                        // Shortcircuit if there's no allowed actions
                        DeleteTopicsResponseData.DeletableTopicResultCollection v = new DeleteTopicsResponseData.DeletableTopicResultCollection();
                        okStates.stream()
                                .map(topicState -> errorResult(apiVersion, topicState, Errors.TOPIC_AUTHORIZATION_FAILED))
                                // using method reference is failing to compile on JDK17, see JDK-8268312
                                .forEach(newElement -> v.mustAdd(newElement));
                        errorStates.stream()
                                .map(topicState -> {
                                    return getDeletableTopicResult(mapping, topicState, apiVersion);
                                })
                                // using method reference is failing to compile on JDK17, see JDK-8268312
                                .forEach(newElement -> v.mustAdd(newElement));
                        return context.requestFilterResultBuilder()
                                .shortCircuitResponse(
                                        new DeleteTopicsResponseData()
                                                .setResponses(v))
                                .completed();
                    }
                    else if (decisions.get(Decision.DENY).isEmpty()) {
                        // Forward if there's no denied actions, but we might need to reinsert lookup errors
                        request.setTopics(okStates);
                        authorizationFilter.pushInflightState(header, (DeleteTopicsResponseData response) -> {
                            response.responses().addAll(errorStates.stream()
                                    .map(topicState -> getDeletableTopicResult(mapping, topicState, apiVersion)).toList());
                            return response;
                        });
                        return context.forwardRequest(header, request);
                    }
                    else {
                        request.setTopics(okStates.stream()
                                .filter(topicState -> authorization.decision(operation, topicState.name()) == Decision.ALLOW)
                                .toList());

                        var denied = decisions.get(Decision.DENY)
                                .stream().map(t -> errorResult(apiVersion, t, Errors.TOPIC_AUTHORIZATION_FAILED))
                                .toList();
                        var errored = errorStates.stream()
                                .map(topicState -> getDeletableTopicResult(mapping, topicState, apiVersion)).toList();
                        authorizationFilter.pushInflightState(header, (DeleteTopicsResponseData response) -> {
                            response.responses().addAll(denied);
                            response.responses().addAll(errored);
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
                                .map(topicName -> errorResult(apiVersion, topicName, Errors.TOPIC_AUTHORIZATION_FAILED))
                                // using method reference is failing to compile on JDK17, see JDK-8268312
                                .forEach(newElement -> v.mustAdd(newElement));
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
                                .stream().map(t -> errorResult(apiVersion, t, Errors.TOPIC_AUTHORIZATION_FAILED))
                                .toList();
                        authorizationFilter.pushInflightState(header, (DeleteTopicsResponseData response) -> {
                            response.responses().addAll(list);
                            return response;
                        });
                        return context.forwardRequest(header, request);
                    }
                }
            });
        });
    }

    private static DeleteTopicsResponseData.DeletableTopicResult getDeletableTopicResult(TopicNameMapping mapping, DeleteTopicsRequestData.DeleteTopicState topicState,
                                                                                         short apiVersion) {
        var exception = mapping.failures().getOrDefault(topicState.topicId(), new TopicNameMappingException(Errors.UNKNOWN_SERVER_ERROR));
        return errorResult(apiVersion, topicState, exception.getError());
    }

    private static @Nullable String topicName(DeleteTopicsRequestData.DeleteTopicState state, Map<Uuid, String> topicIdToName) {
        String topicName;
        if (state.name() != null) {
            topicName = state.name();
        }
        else {
            topicName = topicIdToName.get(state.topicId());
        }
        return topicName;
    }

    static DeleteTopicsResponseData.DeletableTopicResult errorResult(short apiVersion,
                                                                     DeleteTopicsRequestData.DeleteTopicState state,
                                                                     Errors error) {

        if (apiVersion < 6) {
            throw new IllegalStateException();
        }
        return errorResult(apiVersion, new DeleteTopicsResponseData.DeletableTopicResult(), error)
                .setTopicId(state.topicId())
                .setName(state.name());
    }

    static DeleteTopicsResponseData.DeletableTopicResult errorResult(short apiVersion,
                                                                     DeleteTopicsResponseData.DeletableTopicResult topicResult,
                                                                     Errors error) {
        return topicResult
                .setErrorCode(error.code())
                .setErrorMessage(apiVersion >= 5 ? error.message() : null);
    }

    static DeleteTopicsResponseData.DeletableTopicResult errorResult(short apiVersion,
                                                                     String topicName,
                                                                     Errors error) {
        if (apiVersion >= 6) {
            throw new IllegalStateException();
        }
        return errorResult(apiVersion, new DeleteTopicsResponseData.DeletableTopicResult(), error)
                .setName(topicName);
    }
}
