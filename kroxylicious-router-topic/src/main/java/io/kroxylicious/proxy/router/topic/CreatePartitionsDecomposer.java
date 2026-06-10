/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.protocol.Errors;

/**
 * Splits a CREATE_PARTITIONS request by topic ownership and merges
 * the per-route responses.
 */
class CreatePartitionsDecomposer implements RequestDecomposer<CreatePartitionsRequestData, CreatePartitionsResponseData> {

    static final CreatePartitionsDecomposer INSTANCE = new CreatePartitionsDecomposer();

    private CreatePartitionsDecomposer() {
    }

    static final String ASSIGNMENTS_NOT_SUPPORTED_MESSAGE = "Explicit partition assignments are not supported by the topic router";

    @Override
    public Map<String, CreatePartitionsRequestData> decompose(CreatePartitionsRequestData request,
                                                              TopicRoutingTable table,
                                                              short apiVersion) {
        var result = new LinkedHashMap<String, CreatePartitionsRequestData>();
        for (var topic : request.topics()) {
            String route = table.routeForTopic(topic.name());
            if (route != null && !hasAssignments(topic)) {
                result.computeIfAbsent(route, k -> copyEnvelope(request))
                        .topics().add(topic.duplicate());
            }
        }
        return result;
    }

    private static boolean hasAssignments(CreatePartitionsRequestData.CreatePartitionsTopic topic) {
        return topic.assignments() != null && !topic.assignments().isEmpty();
    }

    @Override
    public CreatePartitionsResponseData recompose(Map<String, CreatePartitionsResponseData> responses,
                                                  CreatePartitionsRequestData originalRequest,
                                                  short apiVersion) {
        var merged = new CreatePartitionsResponseData();
        int maxThrottle = 0;
        for (var resp : responses.values()) {
            for (var topicResult : resp.results()) {
                merged.results().add(topicResult.duplicate());
            }
            maxThrottle = Math.max(maxThrottle, resp.throttleTimeMs());
        }
        merged.setThrottleTimeMs(maxThrottle);
        return merged;
    }

    static CreatePartitionsResponseData errorResponseForUnroutableTopics(CreatePartitionsRequestData request,
                                                                         TopicRoutingTable table) {
        var errorResponse = new CreatePartitionsResponseData();
        for (var topic : request.topics()) {
            if (!table.isRoutable(topic.name())) {
                errorResponse.results().add(
                        new CreatePartitionsTopicResult()
                                .setName(topic.name())
                                .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
            }
        }
        return errorResponse;
    }

    static CreatePartitionsResponseData errorResponseForTopicsWithAssignments(
                                                                              CreatePartitionsRequestData request,
                                                                              TopicRoutingTable table) {
        var errorResponse = new CreatePartitionsResponseData();
        for (var topic : request.topics()) {
            if (table.isRoutable(topic.name()) && hasAssignments(topic)) {
                errorResponse.results().add(
                        new CreatePartitionsTopicResult()
                                .setName(topic.name())
                                .setErrorCode(Errors.INVALID_REPLICA_ASSIGNMENT.code())
                                .setErrorMessage(ASSIGNMENTS_NOT_SUPPORTED_MESSAGE));
            }
        }
        return errorResponse;
    }

    private static CreatePartitionsRequestData copyEnvelope(CreatePartitionsRequestData original) {
        var copy = new CreatePartitionsRequestData();
        copy.setTimeoutMs(original.timeoutMs());
        copy.setValidateOnly(original.validateOnly());
        return copy;
    }
}
