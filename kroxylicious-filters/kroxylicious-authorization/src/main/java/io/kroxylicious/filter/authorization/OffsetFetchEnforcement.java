/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
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
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;

import edu.umd.cs.findbugs.annotations.Nullable;

public class OffsetFetchEnforcement extends ApiEnforcement<OffsetFetchRequestData, OffsetFetchResponseData> {
    /*
     * At api version 8 the OffsetFetch API was evolved significantly,
     * introducing an OffsetFetchRequestGroup list at the top level, so that
     * offsets for multiple groups can be retrieved with a single RPC.
     */
    public static final short FIRST_VERSION_USING_GROUP_BATCHING = 8;

    // lowest version supported by proxy
    @Override
    short minSupportedVersion() {
        return 1;
    }

    @Override
    short maxSupportedVersion() {
        return 10;
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

    @Override
    CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                     OffsetFetchResponseData response,
                                                     FilterContext context,
                                                     AuthorizationFilter authorizationFilter) {
        var state = Objects.requireNonNull(authorizationFilter.popInflightState(header, RequestKind.class));
        if (!state.topLevelAllTopics() && state.groupsWithAllTopics().isEmpty()) {
            context.forwardResponse(header, response);
        }
        List<Uuid> topicIds = extractAllTopicIds(response);
        return context.topicNames(topicIds)
                .thenCompose(topicNameMapping -> authorizeResponseTopics(header, response, context, authorizationFilter, topicNameMapping, state));
    }

    private static CompletionStage<ResponseFilterResult> authorizeResponseTopics(ResponseHeaderData header,
                                                                                 OffsetFetchResponseData response,
                                                                                 FilterContext context,
                                                                                 AuthorizationFilter authorizationFilter,
                                                                                 TopicNameMapping topicNameMapping,
                                                                                 RequestKind state) {
        var actions = topicActions(response, topicNameMapping);
        return authorizationFilter.authorization(context, actions)
                .thenCompose(authorization -> {
                    @Nullable
                    List<OffsetFetchResponseTopic> topLevelTopics = response.topics();
                    if (topLevelTopics != null) {
                        applyTopLevelTopicAuthz(state, authorization, topLevelTopics);
                    }
                    @Nullable
                    List<OffsetFetchResponseGroup> groups = response.groups();
                    if (groups != null) {
                        for (var group : groups) {
                            @Nullable
                            List<OffsetFetchResponseTopics> groupLevelTopics = group.topics();
                            if (groupLevelTopics != null) {
                                applyGroupScopedTopicAuthz(state, authorization, group, groupLevelTopics, topicNameMapping);
                            }
                        }
                    }
                    return context.forwardResponse(header, response);
                });
    }

    private static List<Uuid> extractAllTopicIds(OffsetFetchResponseData response) {
        return Optional.ofNullable(response.groups()).stream().flatMap(Collection::stream).flatMap(
                offsetFetchResponseGroup -> offsetFetchResponseGroup.topics().stream()).map(OffsetFetchResponseTopics::topicId)
                .filter(uuid -> !Uuid.ZERO_UUID.equals(uuid))
                .distinct().toList();
    }

    private static void applyTopLevelTopicAuthz(RequestKind state,
                                                AuthorizeResult authorization,
                                                List<OffsetFetchResponseTopic> topLevelTopics) {
        var partition = authorization.partition(topLevelTopics,
                TopicResource.DESCRIBE,
                OffsetFetchResponseTopic::name);
        var denied = partition.get(Decision.DENY);
        if (state.topLevelAllTopics()) {
            // client asked for all topics => we filter out the denied ones
            topLevelTopics.removeAll(denied);
        }
        else {
            // client queried for specific topics, => we must include error codes for those
            for (var t : topLevelTopics) {
                if (denied.contains(t)) {
                    t.setPartitions(t.partitions().stream()
                            .map(p -> OffsetFetchEnforcement.unauthorizedTopLevelPartition(p.partitionIndex()))
                            .toList());
                }
            }
        }
    }

    private static List<Action> topicActions(OffsetFetchResponseData response, TopicNameMapping topicNameMapping) {
        return Stream.concat(
                Optional.ofNullable(response.topics()).stream() // the non-batched case
                        .flatMap(Collection::stream)
                        .map(OffsetFetchResponseTopic::name),
                response.groups().stream() // the batched case
                        .flatMap(g -> Optional.ofNullable(g.topics()).stream())
                        .flatMap(Collection::stream)
                        .map(offsetFetchResponseTopics -> maybeGetName(offsetFetchResponseTopics, topicNameMapping))
                        .filter(Objects::nonNull)
                        .distinct())
                .map(topicName -> new Action(TopicResource.DESCRIBE, topicName))
                .toList();
    }

    private static @Nullable String maybeGetName(OffsetFetchResponseTopics responseTopics, TopicNameMapping topicNameMapping) {
        if (responseTopics.name() != null && !responseTopics.name().isEmpty()) {
            return responseTopics.name();
        }
        else {
            return topicNameMapping.topicNames().get(responseTopics.topicId());
        }
    }

    private static void applyGroupScopedTopicAuthz(RequestKind state,
                                                   AuthorizeResult authorization,
                                                   OffsetFetchResponseGroup group,
                                                   List<OffsetFetchResponseTopics> groupScopedTopics,
                                                   TopicNameMapping topicNameMapping) {
        Map<Boolean, List<OffsetFetchResponseTopics>> byNameKnown = groupScopedTopics.stream()
                .collect(Collectors.partitioningBy(topics -> maybeGetName(topics, topicNameMapping) != null));
        byNameKnown.get(false).forEach(topics -> failAllPartitions(topics, Errors.UNKNOWN_TOPIC_ID));
        var denied = authorization.partition(byNameKnown.get(true),
                TopicResource.DESCRIBE,
                topics -> maybeGetName(topics, topicNameMapping)).get(Decision.DENY);
        if (state.isGroupWithAllTopics(group)) {
            // client asked for all topics => we filter out the denied ones
            groupScopedTopics.removeAll(denied);
        }
        else {
            // client queried for specific topics, => we must include error codes for those
            for (var t : groupScopedTopics) {
                if (denied.contains(t)) {
                    failAllPartitions(t, Errors.TOPIC_AUTHORIZATION_FAILED);
                }
            }
        }
    }

    private static void failAllPartitions(OffsetFetchResponseTopics topics, Errors error) {
        topics.setPartitions(topics.partitions().stream()
                .map(p -> OffsetFetchEnforcement.failedPartition(p.partitionIndex(), error))
                .toList());
    }

    private static OffsetFetchResponsePartition unauthorizedTopLevelPartition(int partitionIndex) {
        return new OffsetFetchResponsePartition()
                .setPartitionIndex(partitionIndex)
                .setCommittedOffset(OffsetFetchResponse.INVALID_OFFSET)
                .setMetadata(OffsetFetchResponse.NO_METADATA)
                .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }

    private static OffsetFetchResponsePartitions failedPartition(Integer partitionIndex, Errors errors) {
        // this looks almost identical to the above method: the difference is plural vs singular of the type names in the method signature
        return new OffsetFetchResponsePartitions()
                .setPartitionIndex(partitionIndex)
                .setCommittedOffset(OffsetFetchResponse.INVALID_OFFSET)
                .setMetadata(OffsetFetchResponse.NO_METADATA)
                .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                .setErrorCode(errors.code());
    }

}
