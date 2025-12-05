/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopic;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopics;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseGroup;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartition;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartitions;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopic;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopics;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.OffsetFetchResponse;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * At api version 8 the OffsetFetch API was evolved significantly, introducing an OffsetFetchRequestGroup list at the top level, so that
 * offsets for multiple groups can be retrieved with a single RPC.
 */
public class OffsetFetchEnforcement extends ApiEnforcement<OffsetFetchRequestData, OffsetFetchResponseData> {
    public static final short FIRST_VERSION_USING_GROUP_BATCHING = 8;
    public static final int LAST_VERSION_WITH_TOPIC_NAMES = 9;

    // lowest version supported by proxy
    @Override
    short minSupportedVersion() {
        return 1;
    }

    // last version before switching to topic ids
    @Override
    short maxSupportedVersion() {
        return LAST_VERSION_WITH_TOPIC_NAMES;
    }

    private static boolean isAllTopics(RequestHeaderData header,
                                       OffsetFetchRequestData request) {
        boolean batched = header.requestApiVersion() >= FIRST_VERSION_USING_GROUP_BATCHING;
        if (batched) {
            return request.groups().stream()
                    .allMatch(ofrq -> ofrq.topics() == null);
        }
        else {
            return request.topics() == null;
        }
    }

    record State(@Nullable List<OffsetFetchResponseTopic> deniedTopics) implements InflightState<OffsetFetchResponseData> {
        // OffsetFetch can have a null list of `topics`, which means "all topics"
        // when `topics` is non-null was can apply authorization on the request
        // and when `topics` is null we have to do authorization on the response
        // we represent the latter case using a null `deniedTopics`.
        boolean wasAllTopicsRequest() {
            return deniedTopics == null;
        }

        @Override
        public OffsetFetchResponseData merge(OffsetFetchResponseData response) {
            response.topics().addAll(Objects.requireNonNull(deniedTopics));
            return response;
        }
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   OffsetFetchRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {

        boolean batched = header.requestApiVersion() >= FIRST_VERSION_USING_GROUP_BATCHING;
        if (isAllTopics(header, request)) {
            authorizationFilter.pushInflightState(header, new State(null));
            return context.forwardRequest(header, request);
        }
        List<Action> actions;
        if (batched) {
            actions = request.groups().stream()
                    .flatMap(ofrq -> ofrq.topics().stream())
                    .map(ofrt -> new Action(TopicResource.DESCRIBE, ofrt.name()))
                    .toList();
        }
        else {
            actions = request.topics().stream()
                    .map(ofrd -> new Action(TopicResource.DESCRIBE, ofrd.name()))
                    .toList();
        }

        return authorizationFilter.authorization(context, actions)
                .thenCompose(authorization -> {
                    if (batched) {
                        var flattenedDecisions = authorization.partition(
                                request.groups().stream()
                                        .flatMap(g -> g.topics().stream())
                                        .toList(),
                                TopicResource.DESCRIBE,
                                OffsetFetchRequestTopics::name);
                        if (flattenedDecisions.get(Decision.DENY).isEmpty()) {
                            authorizationFilter.pushInflightState(header, new State(List.of()));
                            return context.forwardRequest(header, request);
                        }
                        else if (flattenedDecisions.get(Decision.ALLOW).isEmpty()) {
                            return denyAllTopicsResponseForBatched(request, context);
                        }
                        else {
                            return removeDeniedTopicsAndForwardBatched(header, request, context, authorizationFilter, authorization);
                        }
                    }
                    else {
                        var decisions = authorization.partition(request.topics(),
                                TopicResource.DESCRIBE,
                                OffsetFetchRequestTopic::name);
                        var denied = decisions.get(Decision.DENY);
                        var allowed = decisions.get(Decision.ALLOW);
                        if (denied.isEmpty()) {
                            authorizationFilter.pushInflightState(header, new State(List.of()));
                            return context.forwardRequest(header, request);
                        }
                        else if (allowed.isEmpty()) {
                            return denyAllForNonBatched(context, decisions);
                        }
                        else {
                            return removeDeniedTopicsAndForwardNonBatched(header, request, context, authorizationFilter,
                                    allowed,
                                    denied);
                        }
                    }
                });
    }

    private static CompletionStage<RequestFilterResult> removeDeniedTopicsAndForwardNonBatched(RequestHeaderData header,
                                                                                               OffsetFetchRequestData request,
                                                                                               FilterContext context,
                                                                                               AuthorizationFilter authorizationFilter,
                                                                                               List<OffsetFetchRequestTopic> allowed,
                                                                                               List<OffsetFetchRequestTopic> denied) {
        authorizationFilter.pushInflightState(header, new State(createDeniedTopicResponsesNonBatched(denied)));
        request.setTopics(allowed);
        return context.forwardRequest(header, request);
    }

    private static CompletionStage<RequestFilterResult> denyAllForNonBatched(FilterContext context,
                                                                             Map<Decision, List<OffsetFetchRequestTopic>> t1) {
        var offsetFetchResponseTopics = createDeniedTopicResponsesNonBatched(t1.get(Decision.DENY));
        OffsetFetchResponseData response = new OffsetFetchResponseData();
        response.setTopics(offsetFetchResponseTopics);
        return context.requestFilterResultBuilder()
                .shortCircuitResponse(response)
                .completed();
    }

    private static List<OffsetFetchResponseTopic> createDeniedTopicResponsesNonBatched(List<OffsetFetchRequestTopic> offsetFetchRequestTopics) {
        return offsetFetchRequestTopics.stream()
                .map(requestTopic -> new OffsetFetchResponseTopic()
                        .setName(requestTopic.name())
                        .setPartitions(requestTopic.partitionIndexes().stream()
                                .map(OffsetFetchEnforcement::unauthorizedPartitionNonBatched)
                                .toList()))
                .toList();
    }

    private static List<OffsetFetchResponseTopics> createDeniedResponseTopicsBatched(List<OffsetFetchRequestTopics> denied) {
        // this looks almost identical to the above method: the difference is plural vs singular of the type names in the method signature
        return denied.stream()
                .map(requestTopic -> new OffsetFetchResponseTopics()
                        .setName(requestTopic.name())
                        .setPartitions(requestTopic.partitionIndexes().stream()
                                .map(OffsetFetchEnforcement::unauthorizedPartitionBatched)
                                .toList()))
                .toList();
    }

    private static OffsetFetchResponsePartition unauthorizedPartitionNonBatched(int partitionIndex) {
        return new OffsetFetchResponsePartition()
                .setPartitionIndex(partitionIndex)
                .setCommittedOffset(OffsetFetchResponse.INVALID_OFFSET)
                .setMetadata(OffsetFetchResponse.NO_METADATA)
                .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    static OffsetFetchResponsePartitions unauthorizedPartitionBatched(Integer partitionIndex) {
        // this looks almost identical to the above method: the difference is plural vs singular of the type names in the method signature
        return new OffsetFetchResponsePartitions()
                .setPartitionIndex(partitionIndex)
                .setCommittedOffset(OffsetFetchResponse.INVALID_OFFSET)
                .setMetadata(OffsetFetchResponse.NO_METADATA)
                .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    @Override
    CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                     OffsetFetchResponseData response,
                                                     FilterContext context,
                                                     AuthorizationFilter authorizationFilter) {
        var inflightState = authorizationFilter.popInflightState(header, InflightState.class);
        if (inflightState instanceof State state && state.wasAllTopicsRequest()) {
            var actions = Stream.concat(
                    response.topics().stream() // the non-batched case
                            .map(OffsetFetchResponseTopic::name),
                    response.groups().stream() // the batched case
                            .flatMap(g -> g.topics().stream())
                            .map(OffsetFetchResponseTopics::name)
                            .distinct())
                    .map(topicName -> new Action(TopicResource.DESCRIBE, topicName))
                    .toList();
            return authorizationFilter.authorization(context, actions)
                    .thenCompose(authorization -> {
                        if (response.topics() != null) {
                            var partition = authorization.partition(response.topics(),
                                    TopicResource.DESCRIBE,
                                    OffsetFetchResponseTopic::name);
                            response.topics().removeAll(partition.get(Decision.DENY));
                        }
                        if (response.groups() != null) {
                            for (var group : response.groups()) {
                                var partition2 = authorization.partition(group.topics(),
                                        TopicResource.DESCRIBE,
                                        OffsetFetchResponseTopics::name);
                                group.topics().removeAll(partition2.get(Decision.DENY));
                            }
                        }
                        return context.forwardResponse(header, response);
                    });
        }
        else {
            return context.forwardResponse(header, inflightState != null ? ((ApiMessage) inflightState.merge(response)) : response);
        }
    }

    private static CompletionStage<RequestFilterResult> removeDeniedTopicsAndForwardBatched(RequestHeaderData header,
                                                                                            OffsetFetchRequestData request,
                                                                                            FilterContext context,
                                                                                            AuthorizationFilter authorizationFilter,
                                                                                            AuthorizeResult authorization) {
        List<OffsetFetchResponseGroup> toMerge = new ArrayList<>();
        List<OffsetFetchRequestGroup> groupsToRemove = new ArrayList<>();
        for (var requestGroup : request.groups()) {
            Map<Decision, List<OffsetFetchRequestTopics>> partitioned = authorization.partition(requestGroup.topics(),
                    TopicResource.DESCRIBE, OffsetFetchRequestTopics::name);
            var allowed = partitioned.getOrDefault(Decision.ALLOW, List.of());
            var denied = partitioned.getOrDefault(Decision.DENY, List.of());
            if (!denied.isEmpty()) {
                List<OffsetFetchResponseTopics> offsetFetchResponseTopics = createDeniedResponseTopicsBatched(denied);
                toMerge.add(new OffsetFetchResponseGroup()
                        .setGroupId(requestGroup.groupId())
                        .setTopics(offsetFetchResponseTopics));
                if (allowed.isEmpty()) {
                    groupsToRemove.add(requestGroup);
                }
                else {
                    requestGroup.topics().removeAll(denied);
                }
            }
        }
        request.groups().removeAll(groupsToRemove);
        authorizationFilter.pushInflightState(header, (OffsetFetchResponseData response) -> {
            for (OffsetFetchResponseGroup offsetFetchResponseGroup : toMerge) {
                OffsetFetchResponseGroup upstreamResponseGroup = response.groups().stream()
                        .filter(group -> group.groupId().equals(offsetFetchResponseGroup.groupId()))
                        .findFirst().orElse(null);
                if (upstreamResponseGroup != null) {
                    // add to upstream group
                    upstreamResponseGroup.topics().addAll(offsetFetchResponseGroup.topics());
                }
                else {
                    // append entire group
                    response.groups().add(offsetFetchResponseGroup);
                }
            }
            return response;
        });
        return context.forwardRequest(header, request);
    }

    private static CompletionStage<RequestFilterResult> denyAllTopicsResponseForBatched(OffsetFetchRequestData request, FilterContext context) {
        OffsetFetchResponseData response = new OffsetFetchResponseData();
        for (OffsetFetchRequestGroup group : request.groups()) {
            OffsetFetchResponseGroup responseGroup = new OffsetFetchResponseGroup();
            responseGroup.setGroupId(group.groupId());
            List<OffsetFetchResponseTopics> denyAll = createDeniedResponseTopicsBatched(group.topics());
            responseGroup.setTopics(denyAll);
            response.groups().add(responseGroup);
        }
        return context.requestFilterResultBuilder()
                .shortCircuitResponse(response)
                .completed();
    }
}
