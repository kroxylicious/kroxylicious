/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.protocol.Errors;

/**
 * Splits a LIST_OFFSETS request by topic ownership and merges
 * the per-route responses.
 */
class ListOffsetsDecomposer implements RequestDecomposer<ListOffsetsRequestData, ListOffsetsResponseData> {

    static final ListOffsetsDecomposer INSTANCE = new ListOffsetsDecomposer();

    private ListOffsetsDecomposer() {
    }

    @Override
    public Map<String, ListOffsetsRequestData> decompose(ListOffsetsRequestData request,
                                                         TopicRoutingTable table,
                                                         short apiVersion) {
        var result = new LinkedHashMap<String, ListOffsetsRequestData>();
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
    public ListOffsetsResponseData recompose(Map<String, ListOffsetsResponseData> responses,
                                             ListOffsetsRequestData originalRequest) {
        var merged = new ListOffsetsResponseData();
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

    static ListOffsetsResponseData errorResponseForUnroutableTopics(ListOffsetsRequestData request,
                                                                    TopicRoutingTable table) {
        var errorResponse = new ListOffsetsResponseData();
        for (var topic : request.topics()) {
            if (table.routeForTopic(topic.name()) == null) {
                var topicResponse = new ListOffsetsTopicResponse().setName(topic.name());
                for (var partition : topic.partitions()) {
                    topicResponse.partitions().add(
                            new ListOffsetsPartitionResponse()
                                    .setPartitionIndex(partition.partitionIndex())
                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
                }
                errorResponse.topics().add(topicResponse);
            }
        }
        return errorResponse;
    }

    private static ListOffsetsRequestData copyEnvelope(ListOffsetsRequestData original) {
        var copy = new ListOffsetsRequestData();
        copy.setReplicaId(original.replicaId());
        copy.setIsolationLevel(original.isolationLevel());
        return copy;
    }
}
