/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.Errors;

/**
 * Splits an OFFSET_COMMIT request by topic ownership and merges
 * the per-route responses.
 */
class OffsetCommitDecomposer implements RequestDecomposer<OffsetCommitRequestData, OffsetCommitResponseData> {

    static final OffsetCommitDecomposer INSTANCE = new OffsetCommitDecomposer();

    private OffsetCommitDecomposer() {
    }

    @Override
    public Map<String, OffsetCommitRequestData> decompose(OffsetCommitRequestData request,
                                                          TopicRoutingTable table) {
        var result = new LinkedHashMap<String, OffsetCommitRequestData>();
        for (var topic : request.topics()) {
            String route = table.routeForTopic(topic.name());
            if (route != null) {
                result.computeIfAbsent(route, k -> copyEnvelope(request))
                        .topics().add(topic.duplicate());
            }
        }
        return result;
    }

    @Override
    public OffsetCommitResponseData recompose(Map<String, OffsetCommitResponseData> responses,
                                              OffsetCommitRequestData originalRequest) {
        var merged = new OffsetCommitResponseData();
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

    static OffsetCommitResponseData errorResponseForUnroutableTopics(OffsetCommitRequestData request,
                                                                     TopicRoutingTable table) {
        var errorResponse = new OffsetCommitResponseData();
        for (var topic : request.topics()) {
            if (table.routeForTopic(topic.name()) == null) {
                var topicResponse = new OffsetCommitResponseTopic().setName(topic.name());
                for (var partition : topic.partitions()) {
                    topicResponse.partitions().add(
                            new OffsetCommitResponsePartition()
                                    .setPartitionIndex(partition.partitionIndex())
                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
                }
                errorResponse.topics().add(topicResponse);
            }
        }
        return errorResponse;
    }

    private static OffsetCommitRequestData copyEnvelope(OffsetCommitRequestData original) {
        var copy = new OffsetCommitRequestData();
        copy.setGroupId(original.groupId());
        copy.setGenerationIdOrMemberEpoch(original.generationIdOrMemberEpoch());
        copy.setMemberId(original.memberId());
        copy.setGroupInstanceId(original.groupInstanceId());
        copy.setRetentionTimeMs(original.retentionTimeMs());
        return copy;
    }
}
