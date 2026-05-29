/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.protocol.Errors;

/**
 * Splits a DELETE_TOPICS request by topic ownership and merges
 * the per-route responses. Operates on the v0-5 wire format
 * where topics are a flat list of name strings.
 */
class DeleteTopicsDecomposer implements RequestDecomposer<DeleteTopicsRequestData, DeleteTopicsResponseData> {

    static final DeleteTopicsDecomposer INSTANCE = new DeleteTopicsDecomposer();

    private DeleteTopicsDecomposer() {
    }

    @Override
    public Map<String, DeleteTopicsRequestData> decompose(DeleteTopicsRequestData request,
                                                          TopicRoutingTable table) {
        var result = new LinkedHashMap<String, DeleteTopicsRequestData>();
        for (var topicName : request.topicNames()) {
            String route = table.routeForTopic(topicName);
            if (route != null) {
                result.computeIfAbsent(route, k -> copyEnvelope(request))
                        .topicNames().add(topicName);
            }
        }
        return result;
    }

    @Override
    public DeleteTopicsResponseData recompose(Map<String, DeleteTopicsResponseData> responses,
                                              DeleteTopicsRequestData originalRequest) {
        var merged = new DeleteTopicsResponseData();
        int maxThrottle = 0;
        for (var resp : responses.values()) {
            for (var topicResult : resp.responses()) {
                merged.responses().add(topicResult.duplicate());
            }
            maxThrottle = Math.max(maxThrottle, resp.throttleTimeMs());
        }
        merged.setThrottleTimeMs(maxThrottle);
        return merged;
    }

    static DeleteTopicsResponseData errorResponseForUnroutableTopics(DeleteTopicsRequestData request,
                                                                     TopicRoutingTable table) {
        var errorResponse = new DeleteTopicsResponseData();
        for (var topicName : request.topicNames()) {
            if (table.routeForTopic(topicName) == null) {
                errorResponse.responses().add(
                        new DeletableTopicResult()
                                .setName(topicName)
                                .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
            }
        }
        return errorResponse;
    }

    private static DeleteTopicsRequestData copyEnvelope(DeleteTopicsRequestData original) {
        var copy = new DeleteTopicsRequestData();
        copy.setTimeoutMs(original.timeoutMs());
        return copy;
    }
}
