/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseGroup;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartition;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartitions;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopic;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopics;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.OffsetFetchResponse;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

public class OffsetFetchEnforcement extends ApiEnforcement<OffsetFetchRequestData, OffsetFetchResponseData> {
    /*
     * At api version 8 the OffsetFetch API was evolved significantly,
     * introducing an OffsetFetchRequestGroup list at the top level, so that
     * offsets for multiple groups can be retrieved with a single RPC.
     */
    public static final short FIRST_VERSION_USING_GROUP_BATCHING = 8;
    public static final int LAST_VERSION_WITH_TOPIC_NAMES = 9;

    // lowest version supported by proxy
    @Override
    short minSupportedVersion() {
        return 1;
    }

    @Override
    short maxSupportedVersion() {
        return LAST_VERSION_WITH_TOPIC_NAMES;
    }

    /**
     * <p>Since API version 2 requests can either identify specific topics, or use a null list of topics
     * to indicate they're interested in the state of "all topics" consumed by the group.</p>
     *
     * <p>On the request path we capture the kind of query as a State and just forward the request.
     * On the response path we used the capture kind to determine whether the topics in the response
     * should be filtered out (for the "all topics" case), or
     * returned with partition-level TOPIC_AUTHZ_FAILED errors (for the specific topics kind of request).</p>
     * @param topLevelAllTopics
     * @param groupsWithAllTopics
     */
    record RequestKind(boolean topLevelAllTopics,
                       Set<String> groupsWithAllTopics)
            implements InflightState<OffsetFetchResponseData> {

        public boolean isGroupWithAllTopics(OffsetFetchResponseGroup group) {
            return this.groupsWithAllTopics().contains(group.groupId());
        }

        @Override
        public OffsetFetchResponseData merge(OffsetFetchResponseData offsetFetchResponseData) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   OffsetFetchRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {

        RequestKind requestKind;
        boolean batched = header.requestApiVersion() >= FIRST_VERSION_USING_GROUP_BATCHING;
        if (batched) {
            requestKind = new RequestKind(false,
                    request.groups().stream()
                            .filter(ofrq -> ofrq.topics() == null)
                            .map(OffsetFetchRequestGroup::groupId)
                            .collect(Collectors.toSet()));
        }
        else {
            requestKind = new RequestKind(request.topics() == null,
                    Set.of());
        }
        authorizationFilter.pushInflightState(header, requestKind);
        return context.forwardRequest(header, request);
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
        var state = Objects.requireNonNull(authorizationFilter.popInflightState(header, RequestKind.class));
        if (!state.topLevelAllTopics() && state.groupsWithAllTopics().isEmpty()) {
            context.forwardResponse(header, response);
        }

        var actions = Stream.concat(
                Optional.ofNullable(response.topics()).stream() // the non-batched case
                        .flatMap(Collection::stream)
                        .map(OffsetFetchResponseTopic::name),
                response.groups().stream() // the batched case
                        .flatMap(g -> Optional.ofNullable(g.topics()).stream())
                        .flatMap(Collection::stream)
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
                        var denied = partition.get(Decision.DENY);
                        if (state.topLevelAllTopics()) { // client asked for all topics => we filter out the denied ones
                            response.topics().removeAll(denied);
                        }
                        else { // client queried for specific topics, => we must include error codes for those
                            for (var t : response.topics()) {
                                if (denied.contains(t)) {
                                    t.setPartitions(t.partitions().stream()
                                            .map(p -> OffsetFetchEnforcement.unauthorizedPartitionNonBatched(p.partitionIndex()))
                                            .toList());
                                }
                            }
                        }
                    }
                    if (response.groups() != null) {
                        for (var group : response.groups()) {
                            if (group.topics() != null) {
                                var denied = authorization.partition(group.topics(),
                                        TopicResource.DESCRIBE,
                                        OffsetFetchResponseTopics::name).get(Decision.DENY);
                                if (state.isGroupWithAllTopics(group)) { // client asked for all topics => we filter out the denied ones
                                    group.topics().removeAll(denied);
                                }
                                else { // client queried for specific topics, => we must include error codes for those
                                    for (var t : group.topics()) {
                                        if (denied.contains(t)) {
                                            t.setPartitions(t.partitions().stream()
                                                    .map(p -> OffsetFetchEnforcement.unauthorizedPartitionBatched(p.partitionIndex()))
                                                    .toList());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    return context.forwardResponse(header, response);
                });

    }

}
