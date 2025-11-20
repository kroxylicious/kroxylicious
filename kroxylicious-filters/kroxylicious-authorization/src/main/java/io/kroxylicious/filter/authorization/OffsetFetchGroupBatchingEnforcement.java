/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.OffsetFetchResponse;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

/**
 * At api version 8 the OffsetFetch API was evolved significantly, introducing an OffsetFetchRequestGroup list at the top level, so that
 * offsets for multiple groups can be retrieved with a single RPC. The generated java types and merge logic are different enough
 * to justify a distinct enforcer implementation.
 */
public class OffsetFetchGroupBatchingEnforcement extends ApiEnforcement<OffsetFetchRequestData, OffsetFetchResponseData> {

    public static final short FIRST_VERSION_USING_GROUP_BATCHING = 8;

    @Override
    short minSupportedVersion() {
        return FIRST_VERSION_USING_GROUP_BATCHING;
    }

    // last version before switching to topic ids
    @Override
    short maxSupportedVersion() {
        return 9;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   OffsetFetchRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        List<Action> actions = request.groups().stream()
                .flatMap(ofrq -> ofrq.topics().stream())
                .map(ofrt -> new Action(TopicResource.DESCRIBE, ofrt.name()))
                .toList();
        return authorizationFilter.authorization(context, actions)
                .thenCompose(authorization -> {
                    var flattenedDecisions = authorization.partition(request.groups().stream()
                            .flatMap(g -> g.topics().stream())
                            .toList(), TopicResource.DESCRIBE, OffsetFetchRequestData.OffsetFetchRequestTopics::name);
                    if (flattenedDecisions.get(Decision.DENY).isEmpty()) {
                        return context.forwardRequest(header, request);
                    }
                    else if (flattenedDecisions.get(Decision.ALLOW).isEmpty()) {
                        return denyAllTopicsResponse(request, context);
                    }
                    else {
                        return removeDeniedTopicsForwardAndMergeResult(header, request, context, authorizationFilter, authorization);
                    }
                });
    }

    private static CompletionStage<RequestFilterResult> removeDeniedTopicsForwardAndMergeResult(RequestHeaderData header, OffsetFetchRequestData request,
                                                                                                FilterContext context, AuthorizationFilter authorizationFilter,
                                                                                                AuthorizeResult authorization) {
        List<OffsetFetchResponseData.OffsetFetchResponseGroup> toMerge = new ArrayList<>();
        List<OffsetFetchRequestData.OffsetFetchRequestGroup> groupsToRemove = new ArrayList<>();
        for (var requestGroup : request.groups()) {
            Map<Decision, List<OffsetFetchRequestData.OffsetFetchRequestTopics>> partitioned = authorization.partition(requestGroup.topics(),
                    TopicResource.DESCRIBE, OffsetFetchRequestData.OffsetFetchRequestTopics::name);
            var allowed = partitioned.getOrDefault(Decision.ALLOW, List.of());
            var denied = partitioned.getOrDefault(Decision.DENY, List.of());
            if (!denied.isEmpty()) {
                List<OffsetFetchResponseData.OffsetFetchResponseTopics> offsetFetchResponseTopics = createDeniedResponseTopics(denied);
                toMerge.add(new OffsetFetchResponseData.OffsetFetchResponseGroup()
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
            for (OffsetFetchResponseData.OffsetFetchResponseGroup offsetFetchResponseGroup : toMerge) {
                OffsetFetchResponseData.OffsetFetchResponseGroup upstreamResponseGroup = response.groups().stream()
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

    private static CompletionStage<RequestFilterResult> denyAllTopicsResponse(OffsetFetchRequestData request, FilterContext context) {
        OffsetFetchResponseData response = new OffsetFetchResponseData();
        for (OffsetFetchRequestData.OffsetFetchRequestGroup group : request.groups()) {
            OffsetFetchResponseData.OffsetFetchResponseGroup responseGroup = new OffsetFetchResponseData.OffsetFetchResponseGroup();
            responseGroup.setGroupId(group.groupId());
            List<OffsetFetchResponseData.OffsetFetchResponseTopics> denyAll = createDeniedResponseTopics(group.topics());
            responseGroup.setTopics(denyAll);
            response.groups().add(responseGroup);
        }
        return context.requestFilterResultBuilder()
                .shortCircuitResponse(response)
                .completed();
    }

    private static List<OffsetFetchResponseData.OffsetFetchResponseTopics> createDeniedResponseTopics(List<OffsetFetchRequestData.OffsetFetchRequestTopics> denied) {
        return denied.stream().map(x -> new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName(x.name())
                .setPartitions(x.partitionIndexes().stream()
                        .map(OffsetFetchGroupBatchingEnforcement::unauthorizedPartition)
                        .toList()))
                .toList();
    }

    static OffsetFetchResponseData.OffsetFetchResponsePartitions unauthorizedPartition(Integer partitionIndex) {
        return new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(partitionIndex)
                .setCommittedOffset(OffsetFetchResponse.INVALID_OFFSET)
                .setMetadata(OffsetFetchResponse.NO_METADATA)
                .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }
}
