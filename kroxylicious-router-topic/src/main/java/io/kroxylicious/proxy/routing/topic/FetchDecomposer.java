/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.Errors;

/**
 * Splits a FETCH request by topic ownership and merges the per-route
 * responses. Fetch sessions are forced off: sub-requests always use
 * {@code sessionId=0, sessionEpoch=-1}.
 */
class FetchDecomposer implements RequestDecomposer<FetchRequestData, FetchResponseData> {

    static final FetchDecomposer INSTANCE = new FetchDecomposer();

    private FetchDecomposer() {
    }

    @Override
    public Map<String, FetchRequestData> decompose(FetchRequestData request,
                                                   TopicRoutingTable table) {
        var result = new LinkedHashMap<String, FetchRequestData>();
        for (var topic : request.topics()) {
            String route = table.routeForTopic(topic.topic());
            if (route != null) {
                result.computeIfAbsent(route, k -> copyEnvelope(request))
                        .topics().add(topic.duplicate());
            }
        }
        return result;
    }

    @Override
    public FetchResponseData recompose(Map<String, FetchResponseData> responses,
                                       FetchRequestData originalRequest) {
        var merged = new FetchResponseData();
        int maxThrottle = 0;
        for (var resp : responses.values()) {
            for (var topic : resp.responses()) {
                merged.responses().add(topic.duplicate());
            }
            maxThrottle = Math.max(maxThrottle, resp.throttleTimeMs());
        }
        merged.setThrottleTimeMs(maxThrottle);
        merged.setSessionId(0);
        merged.setErrorCode(Errors.NONE.code());
        return merged;
    }

    static FetchResponseData errorResponseForUnroutableTopics(FetchRequestData request,
                                                              TopicRoutingTable table) {
        var errorResponse = new FetchResponseData();
        for (var topic : request.topics()) {
            if (table.routeForTopic(topic.topic()) == null) {
                var topicResponse = new FetchableTopicResponse().setTopic(topic.topic());
                for (var partition : topic.partitions()) {
                    topicResponse.partitions().add(
                            new PartitionData()
                                    .setPartitionIndex(partition.partition())
                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
                }
                errorResponse.responses().add(topicResponse);
            }
        }
        return errorResponse;
    }

    private static FetchRequestData copyEnvelope(FetchRequestData original) {
        var copy = new FetchRequestData();
        copy.setMaxWaitMs(original.maxWaitMs());
        copy.setMinBytes(original.minBytes());
        copy.setMaxBytes(original.maxBytes());
        copy.setIsolationLevel(original.isolationLevel());
        copy.setRackId(original.rackId());
        copy.setReplicaId(-1);
        copy.setSessionId(0);
        copy.setSessionEpoch(-1);
        return copy;
    }
}
