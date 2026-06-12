/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseGroup;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartition;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartitions;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopic;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopics;
import org.apache.kafka.common.protocol.Errors;

/**
 * Splits an OFFSET_FETCH request by topic ownership and merges
 * the per-route responses. Handles both v1-7 (single group) and
 * v8-9 (multi-group) wire formats.
 */
class OffsetFetchDecomposer {

    static final OffsetFetchDecomposer INSTANCE = new OffsetFetchDecomposer();

    private OffsetFetchDecomposer() {
    }

    Map<String, OffsetFetchRequestData> decompose(OffsetFetchRequestData request,
                                                  TopicRoutingTable table,
                                                  short apiVersion) {
        if (apiVersion <= 7) {
            return decomposeV1to7(request, table);
        }
        else {
            return decomposeV8plus(request, table);
        }
    }

    OffsetFetchResponseData recompose(Map<String, OffsetFetchResponseData> responses,
                                      OffsetFetchRequestData originalRequest,
                                      short apiVersion) {
        if (apiVersion <= 7) {
            return recomposeV1to7(responses);
        }
        else {
            return recomposeV8plus(responses);
        }
    }

    static OffsetFetchResponseData errorResponseForUnroutableTopics(OffsetFetchRequestData request,
                                                                    TopicRoutingTable table,
                                                                    short apiVersion) {
        if (apiVersion <= 7) {
            return errorResponseV1to7(request, table);
        }
        else {
            return errorResponseV8plus(request, table);
        }
    }

    // --- v1-7: single group with top-level GroupId + Topics ---

    private Map<String, OffsetFetchRequestData> decomposeV1to7(OffsetFetchRequestData request,
                                                               TopicRoutingTable table) {
        var result = new LinkedHashMap<String, OffsetFetchRequestData>();
        if (request.topics() == null) {
            return result;
        }
        for (var topic : request.topics()) {
            String route = table.routeForTopic(topic.name());
            if (route != null) {
                result.computeIfAbsent(route, k -> copyEnvelopeV1to7(request))
                        .topics().add(topic.duplicate());
            }
        }
        return result;
    }

    private OffsetFetchResponseData recomposeV1to7(Map<String, OffsetFetchResponseData> responses) {
        var merged = new OffsetFetchResponseData();
        int maxThrottle = 0;
        for (var resp : responses.values()) {
            for (var topic : resp.topics()) {
                merged.topics().add(topic.duplicate());
            }
            maxThrottle = Math.max(maxThrottle, resp.throttleTimeMs());
        }
        merged.setThrottleTimeMs(maxThrottle);
        return merged;
    }

    private static OffsetFetchResponseData errorResponseV1to7(OffsetFetchRequestData request,
                                                              TopicRoutingTable table) {
        var errorResponse = new OffsetFetchResponseData();
        if (request.topics() == null) {
            return errorResponse;
        }
        for (var topic : request.topics()) {
            if (table.routeForTopic(topic.name()) == null) {
                var topicResponse = new OffsetFetchResponseTopic().setName(topic.name());
                for (int partitionIndex : topic.partitionIndexes()) {
                    topicResponse.partitions().add(
                            new OffsetFetchResponsePartition()
                                    .setPartitionIndex(partitionIndex)
                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                                    .setCommittedOffset(-1));
                }
                errorResponse.topics().add(topicResponse);
            }
        }
        return errorResponse;
    }

    private static OffsetFetchRequestData copyEnvelopeV1to7(OffsetFetchRequestData original) {
        var copy = new OffsetFetchRequestData();
        copy.setGroupId(original.groupId());
        copy.setRequireStable(original.requireStable());
        return copy;
    }

    // --- v8-9: multi-group with Groups array ---

    private Map<String, OffsetFetchRequestData> decomposeV8plus(OffsetFetchRequestData request,
                                                                TopicRoutingTable table) {
        var result = new LinkedHashMap<String, OffsetFetchRequestData>();
        for (var group : request.groups()) {
            if (group.topics() == null) {
                continue;
            }
            for (var topic : group.topics()) {
                String route = table.routeForTopic(topic.name());
                if (route != null) {
                    var routeReq = result.computeIfAbsent(route, k -> new OffsetFetchRequestData()
                            .setRequireStable(request.requireStable()));
                    var routeGroup = findOrCreateGroup(routeReq, group);
                    routeGroup.topics().add(topic.duplicate());
                }
            }
        }
        return result;
    }

    private OffsetFetchResponseData recomposeV8plus(Map<String, OffsetFetchResponseData> responses) {
        var merged = new OffsetFetchResponseData();
        int maxThrottle = 0;
        Map<String, OffsetFetchResponseGroup> groupsByGroupId = new LinkedHashMap<>();

        for (var resp : responses.values()) {
            maxThrottle = Math.max(maxThrottle, resp.throttleTimeMs());
            for (var group : resp.groups()) {
                var mergedGroup = groupsByGroupId.computeIfAbsent(group.groupId(), gid -> {
                    var g = new OffsetFetchResponseGroup();
                    g.setGroupId(gid);
                    g.setErrorCode(group.errorCode());
                    return g;
                });
                for (var topic : group.topics()) {
                    mergedGroup.topics().add(topic.duplicate());
                }
            }
        }

        merged.setThrottleTimeMs(maxThrottle);
        for (var group : groupsByGroupId.values()) {
            merged.groups().add(group);
        }
        return merged;
    }

    private static OffsetFetchResponseData errorResponseV8plus(OffsetFetchRequestData request,
                                                               TopicRoutingTable table) {
        var errorResponse = new OffsetFetchResponseData();
        for (var group : request.groups()) {
            if (group.topics() == null) {
                continue;
            }
            OffsetFetchResponseGroup errorGroup = null;
            for (var topic : group.topics()) {
                if (table.routeForTopic(topic.name()) == null) {
                    if (errorGroup == null) {
                        errorGroup = new OffsetFetchResponseGroup()
                                .setGroupId(group.groupId());
                    }
                    var topicResponse = new OffsetFetchResponseTopics().setName(topic.name());
                    for (int partitionIndex : topic.partitionIndexes()) {
                        topicResponse.partitions().add(
                                new OffsetFetchResponsePartitions()
                                        .setPartitionIndex(partitionIndex)
                                        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                                        .setCommittedOffset(-1));
                    }
                    errorGroup.topics().add(topicResponse);
                }
            }
            if (errorGroup != null) {
                errorResponse.groups().add(errorGroup);
            }
        }
        return errorResponse;
    }

    private static OffsetFetchRequestGroup findOrCreateGroup(OffsetFetchRequestData routeReq,
                                                             OffsetFetchRequestGroup originalGroup) {
        for (var existing : routeReq.groups()) {
            if (existing.groupId().equals(originalGroup.groupId())) {
                return existing;
            }
        }
        var newGroup = new OffsetFetchRequestGroup()
                .setGroupId(originalGroup.groupId())
                .setMemberId(originalGroup.memberId())
                .setMemberEpoch(originalGroup.memberEpoch());
        routeReq.groups().add(newGroup);
        return newGroup;
    }
}
