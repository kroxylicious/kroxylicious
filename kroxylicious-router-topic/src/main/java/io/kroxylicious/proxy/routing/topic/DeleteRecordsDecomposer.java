/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResult;
import org.apache.kafka.common.protocol.Errors;

/**
 * Splits a DELETE_RECORDS request by topic ownership and merges
 * the per-route responses.
 */
class DeleteRecordsDecomposer implements RequestDecomposer<DeleteRecordsRequestData, DeleteRecordsResponseData> {

    static final DeleteRecordsDecomposer INSTANCE = new DeleteRecordsDecomposer();

    private DeleteRecordsDecomposer() {
    }

    @Override
    public Map<String, DeleteRecordsRequestData> decompose(DeleteRecordsRequestData request,
                                                           TopicRoutingTable table) {
        var result = new LinkedHashMap<String, DeleteRecordsRequestData>();
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
    public DeleteRecordsResponseData recompose(Map<String, DeleteRecordsResponseData> responses,
                                               DeleteRecordsRequestData originalRequest) {
        var merged = new DeleteRecordsResponseData();
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

    static DeleteRecordsResponseData errorResponseForUnroutableTopics(DeleteRecordsRequestData request,
                                                                      TopicRoutingTable table) {
        var errorResponse = new DeleteRecordsResponseData();
        for (var topic : request.topics()) {
            if (table.routeForTopic(topic.name()) == null) {
                var topicResult = new DeleteRecordsTopicResult().setName(topic.name());
                for (var partition : topic.partitions()) {
                    topicResult.partitions().add(
                            new DeleteRecordsPartitionResult()
                                    .setPartitionIndex(partition.partitionIndex())
                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
                }
                errorResponse.topics().add(topicResult);
            }
        }
        return errorResponse;
    }

    private static DeleteRecordsRequestData copyEnvelope(DeleteRecordsRequestData original) {
        var copy = new DeleteRecordsRequestData();
        copy.setTimeoutMs(original.timeoutMs());
        return copy;
    }
}
