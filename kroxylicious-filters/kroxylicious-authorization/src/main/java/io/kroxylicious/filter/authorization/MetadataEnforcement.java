/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

public class MetadataEnforcement extends ApiEnforcement<MetadataRequestData, MetadataResponseData> {
    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 13;
    }

    private boolean isAllTopics(RequestHeaderData header, MetadataRequestData metadataRequest) {
        return (metadataRequest.topics() == null) ||
                (metadataRequest.topics().isEmpty() && header.requestApiVersion() == 0);
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   MetadataRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        var includeClusterAuthorizedOperations = header.requestApiVersion() >= 8
                && header.requestApiVersion() <= 10
                && request.includeClusterAuthorizedOperations();
        var includeTopicAuthorizedOperations = request.includeTopicAuthorizedOperations();
        var isAllTopics = isAllTopics(header, request);
        var requestUsesTopicIds = request.topics() != null
                && !request.topics().isEmpty()
                && request.topics().get(0).topicId() != null
                && !request.topics().get(0).topicId().equals(Uuid.ZERO_UUID);

        authorizationFilter.pushInflightState(header,
                new MetadataCompleter(includeClusterAuthorizedOperations,
                        includeTopicAuthorizedOperations,
                        isAllTopics,
                        requestUsesTopicIds,
                        new ArrayList<>()));

        // A metadata request is idempotent EXCEPT when topic creation is allowed.
        // Therefore, it's safe to forward the requests with allowAutoTopicCreation=false as-is
        // (and leave the response handler to filter out topics disallowed by the authorizer),
        // EXCEPT when the request allows topic creation.
        if (!requestUsesTopicIds &&
                (isAllTopics // An all-topics query won't create topics even if the flag is set
                        || !request.allowAutoTopicCreation())) {
            return context.forwardRequest(header, request);
        }

        return onNonIdempotentMetadataRequest(header, request, context, authorizationFilter);
    }

    private CompletionStage<RequestFilterResult> onNonIdempotentMetadataRequest(RequestHeaderData header,
                                                                                MetadataRequestData request,
                                                                                FilterContext context,
                                                                                AuthorizationFilter authorizationFilter) {
        // Problem: If we forward a allowAutoTopicCreation=true request without filtering we could create
        // topics which the user is not allowed to create. But if we filter them out, and they do exist,
        // then we can't return their metadata.
        // Solution: We forward the request with allowAutoTopicCreation=false, filter the response with
        // our authorizer and then forward it again without the _disallowed_ topic creations.
        // Merge the two responses and return to the client.
        // Subtlety: V4 added the flag, before that all requests are allowAutoTopicCreation=true
        // Solution: Always use a version >= 4 for the initial request, and revert to the client's chosen
        // version for the subsequent request.

        var initialRequestHeader = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.METADATA.id)
                .setRequestApiVersion(authorizationFilter.useMetadataVersion() != -1 ? authorizationFilter.useMetadataVersion() : ApiKeys.METADATA.latestVersion()) // Should support topic ids
                .setClientId(header.clientId());
        var initialRequest = new MetadataRequestData()
                .setTopics(request.topics())
                .setAllowAutoTopicCreation(false);
        return context.sendRequest(initialRequestHeader, initialRequest)
                .thenCompose(notCreateResponse -> {
                    var notCreateMetadataResponse = (MetadataResponseData) notCreateResponse;
                    var responseTopicsByExistence = notCreateMetadataResponse.topics().stream()
                            .collect(Collectors.partitioningBy(responseTopic -> Errors.UNKNOWN_TOPIC_OR_PARTITION.code() == responseTopic.errorCode()));
                    var notExistingTopics = responseTopicsByExistence.get(true);
                    var alreadyExistingTopics = responseTopicsByExistence.get(false);
                    var createAndDescribeActions = notExistingTopics.stream()
                            .flatMap(responseTopic -> Stream.of(
                                    new Action(TopicResource.CREATE, responseTopic.name()),
                                    new Action(TopicResource.DESCRIBE, responseTopic.name())))
                            .toList();
                    return authorizationFilter.authorization(context, createAndDescribeActions)
                            .thenCompose(createAndDescribeAuthorization -> {
                                var createDecisions = notExistingTopics.stream()
                                        .flatMap(responseTopic -> request.topics().stream().filter(x -> Objects.equals(responseTopic.name(), x.name())).findFirst()
                                                .stream())
                                        .collect(Collectors
                                                .groupingBy(responseTopic -> createAndDescribeAuthorization.decision(TopicResource.CREATE, responseTopic.name())));
                                var notExistingAndAllowedToCreate = createDecisions.getOrDefault(Decision.ALLOW, List.of());
                                var notExistingAndNotAllowedToCreate = createDecisions.getOrDefault(Decision.DENY, List.of());
                                var additionalResponseTopics = notExistingAndNotAllowedToCreate.stream()
                                        .map(
                                                requestTopic -> {
                                                    var decision = createAndDescribeAuthorization.decision(TopicResource.DESCRIBE, requestTopic.name());
                                                    return new MetadataResponseData.MetadataResponseTopic()
                                                            .setName(requestTopic.name())
                                                            .setErrorCode(
                                                                    (decision == Decision.ALLOW ? Errors.UNKNOWN_TOPIC_OR_PARTITION : Errors.TOPIC_AUTHORIZATION_FAILED)
                                                                            .code());
                                                })
                                        .toList();
                                MetadataCompleter metadataCompleter = authorizationFilter.peekInflightState(header.correlationId(), MetadataCompleter.class);
                                metadataCompleter
                                        .topics()
                                        .addAll(additionalResponseTopics);

                                // We include the topics which we don't need to create,
                                // so that the eventual response is equally fresh for those
                                // as for the topics which are being created
                                var knownTopicNames = alreadyExistingTopics.stream()
                                        .map(MetadataResponseData.MetadataResponseTopic::name)
                                        .collect(Collectors.toSet());
                                var knownTopicIds = alreadyExistingTopics.stream()
                                        .map(MetadataResponseData.MetadataResponseTopic::topicId)
                                        .collect(Collectors.toSet());
                                var concat = notExistingAndAllowedToCreate.stream();
                                if (request.topics() != null) {
                                    concat = Stream.concat(concat,
                                            request.topics().stream()
                                                    .filter(requestTopic -> metadataCompleter.requestUsesTopicIds() ? knownTopicIds.contains(requestTopic.topicId())
                                                            : knownTopicNames.contains(requestTopic.name())));
                                }

                                // We can now forward the client's original request, albeit with a filtered list of topics
                                var requestWithCreateTopics = concat.toList();
                                return context.forwardRequest(header, request.setTopics(requestWithCreateTopics));
                            });
                });
    }

    @Override
    CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                     MetadataResponseData response,
                                                     FilterContext context,
                                                     AuthorizationFilter authorizationFilter) {
        var completer = authorizationFilter.popInflightState(header, MetadataCompleter.class);
        List<Action> actions = new ArrayList<>();
        if (completer.includeClusterAuthorizedOperations()) {
            for (var clusterOp : ClusterResource.values()) {
                actions.add(new Action(clusterOp, ""));
            }
        }
        if (completer.includeTopicAuthorizedOperations()) {
            actions.addAll(response.topics().stream()
                    .map(MetadataResponseData.MetadataResponseTopic::name)
                    .filter(Objects::nonNull)
                    .flatMap(topicName -> Arrays.stream(TopicResource.values()).map(op -> new Action(op, topicName)))
                    .toList());
        }
        else {
            actions.addAll(response.topics().stream()
                    .map(MetadataResponseData.MetadataResponseTopic::name)
                    .filter(Objects::nonNull)
                    .map(topicName -> new Action(TopicResource.DESCRIBE, topicName))
                    .toList());
        }

        return authorizationFilter.authorization(context, actions)
                .thenCompose(authorize -> {
                    var toRemove = new ArrayList<MetadataResponseData.MetadataResponseTopic>();

                    for (var t : response.topics()) {
                        if (authorize.decision(TopicResource.DESCRIBE, t.name()) == Decision.DENY) {
                            if (completer.isAllTopics()) {
                                toRemove.add(t);
                            }
                            else {
                                // TODO in this case do we return the topic Id if the client didn't already know it?
                                t.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
                                t.partitions().clear();
                                t.setIsInternal(false);
                                t.setTopicAuthorizedOperations(Integer.MIN_VALUE);
                                if (completer.requestUsesTopicIds()) {
                                    t.setName(null);
                                }
                                else {
                                    t.setTopicId(Uuid.ZERO_UUID);
                                }
                            }
                        }
                        else { // ALLOW
                            if (completer.includeTopicAuthorizedOperations()) {
                                t.setTopicAuthorizedOperations(topicAuthzOptions(authorize, t));
                            }
                        }
                    }

                    if (completer.includeClusterAuthorizedOperations()) {
                        response.setClusterAuthorizedOperations(clusterAuthzOptions(authorize));
                    }
                    response.topics().removeAll(toRemove);
                    return context.forwardResponse(header, completer.merge(response));
                });
    }

    private static int clusterAuthzOptions(AuthorizeResult authorize) {
        int flag = 0;
        for (var clusterOp : ClusterResource.values()) {
            if (authorize.decision(clusterOp, "") == Decision.ALLOW) {
                flag |= (0x1 << clusterOp.kafkaOrdinal);
            }
        }
        return flag;
    }

    private static int topicAuthzOptions(AuthorizeResult authorize, MetadataResponseData.MetadataResponseTopic t) {
        int flag = 0;
        for (var clusterOp : TopicResource.values()) {
            if (authorize.decision(clusterOp, t.name()) == Decision.ALLOW) {
                flag |= (0x1 << clusterOp.kafkaOrdinal);
            }
        }
        return flag;
    }

    static record MetadataCompleter(boolean includeClusterAuthorizedOperations,
                                    boolean includeTopicAuthorizedOperations,
                                    boolean isAllTopics,
                                    boolean requestUsesTopicIds,
                                    List<MetadataResponseData.MetadataResponseTopic> topics)
            implements InflightState<MetadataResponseData> {
        @Override
        public MetadataResponseData merge(MetadataResponseData response) {
            if (!topics.isEmpty()) {
                response.topics().addAll(topics);
            }
            return response;
        }
    }
}
