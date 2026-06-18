/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.protocol.Errors;

/**
 * Splits a DELETE_TOPICS request by topic ownership and merges
 * the per-route responses. Handles both v0-5 (flat name list) and
 * v6+ (struct array with name and topicId) wire formats.
 */
class DeleteTopicsDecomposer implements RequestDecomposer<DeleteTopicsRequestData, DeleteTopicsResponseData> {

    static final DeleteTopicsDecomposer INSTANCE = new DeleteTopicsDecomposer();

    private DeleteTopicsDecomposer() {
    }

    @Override
    public Map<String, DeleteTopicsRequestData> decompose(DeleteTopicsRequestData request,
                                                          TopicRoutingTable table,
                                                          short apiVersion,
                                                          Function<Uuid, String> topicNameResolver) {
        if (apiVersion >= 6) {
            return decomposeV6(request, table, topicNameResolver);
        }
        return decomposeV0(request, table);
    }

    private Map<String, DeleteTopicsRequestData> decomposeV0(DeleteTopicsRequestData request,
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

    private Map<String, DeleteTopicsRequestData> decomposeV6(DeleteTopicsRequestData request,
                                                             TopicRoutingTable table,
                                                             Function<Uuid, String> topicNameResolver) {
        var result = new LinkedHashMap<String, DeleteTopicsRequestData>();
        for (var topic : request.topics()) {
            String name = topic.name();
            if ((name == null || name.isEmpty()) && !Uuid.ZERO_UUID.equals(topic.topicId())) {
                name = topicNameResolver.apply(topic.topicId());
            }
            if (name != null && !name.isEmpty()) {
                String route = table.routeForTopic(name);
                if (route != null) {
                    result.computeIfAbsent(route, k -> copyEnvelope(request))
                            .topics().add(topic.duplicate());
                }
            }
        }
        return result;
    }

    @Override
    public DeleteTopicsResponseData recompose(Map<String, DeleteTopicsResponseData> responses,
                                              DeleteTopicsRequestData originalRequest,
                                              short apiVersion) {
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
                                                                     TopicRoutingTable table,
                                                                     short apiVersion) {
        if (apiVersion >= 6) {
            return errorResponseV6(request, table);
        }
        return errorResponseV0(request, table);
    }

    private static DeleteTopicsResponseData errorResponseV0(DeleteTopicsRequestData request,
                                                            TopicRoutingTable table) {
        var errorResponse = new DeleteTopicsResponseData();
        for (var topicName : request.topicNames()) {
            if (!table.isRoutable(topicName)) {
                errorResponse.responses().add(
                        new DeletableTopicResult()
                                .setName(topicName)
                                .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
            }
        }
        return errorResponse;
    }

    private static DeleteTopicsResponseData errorResponseV6(DeleteTopicsRequestData request,
                                                            TopicRoutingTable table) {
        var errorResponse = new DeleteTopicsResponseData();
        for (var topic : request.topics()) {
            if (!table.isRoutable(topic.name())) {
                boolean hasName = topic.name() != null && !topic.name().isEmpty();
                var result = new DeletableTopicResult();
                if (!Uuid.ZERO_UUID.equals(topic.topicId())) {
                    result.setTopicId(topic.topicId());
                }
                if (hasName) {
                    result.setName(topic.name());
                    result.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
                }
                else {
                    result.setErrorCode(Errors.UNKNOWN_TOPIC_ID.code());
                }
                errorResponse.responses().add(result);
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
