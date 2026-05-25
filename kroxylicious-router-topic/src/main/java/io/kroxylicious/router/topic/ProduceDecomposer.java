/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;
import org.apache.kafka.common.protocol.Errors;

/**
 * Splits a batched PRODUCE request by topic ownership and merges
 * the per-route responses. Topics that cannot be routed produce
 * a synthetic {@link Errors#UNKNOWN_TOPIC_OR_PARTITION} response.
 */
class ProduceDecomposer implements RequestDecomposer<ProduceRequestData, ProduceResponseData> {

    static final ProduceDecomposer INSTANCE = new ProduceDecomposer();

    private ProduceDecomposer() {
    }

    @Override
    public Map<String, ProduceRequestData> decompose(ProduceRequestData request,
                                                     TopicRoutingTable table) {
        var result = new LinkedHashMap<String, ProduceRequestData>();
        for (var td : request.topicData()) {
            String route = table.routeForTopic(td.name());
            if (route != null) {
                result.computeIfAbsent(route, k -> copyEnvelope(request))
                        .topicData().add(td.duplicate());
            }
        }
        return result;
    }

    @Override
    public ProduceResponseData recompose(Map<String, ProduceResponseData> responses,
                                         ProduceRequestData originalRequest) {
        var merged = new ProduceResponseData();
        int maxThrottle = 0;
        for (var resp : responses.values()) {
            for (var tr : resp.responses()) {
                merged.responses().add(tr.duplicate());
            }
            for (var ne : resp.nodeEndpoints()) {
                merged.nodeEndpoints().add(ne.duplicate());
            }
            maxThrottle = Math.max(maxThrottle, resp.throttleTimeMs());
        }
        merged.setThrottleTimeMs(maxThrottle);
        return merged;
    }

    /**
     * Builds a synthetic error response for topics that have no route.
     */
    static ProduceResponseData errorResponseForUnroutableTopics(ProduceRequestData request,
                                                                TopicRoutingTable table) {
        var errorResponse = new ProduceResponseData();
        for (var td : request.topicData()) {
            if (table.routeForTopic(td.name()) == null) {
                var topicResponse = new TopicProduceResponse().setName(td.name());
                for (var pd : td.partitionData()) {
                    topicResponse.partitionResponses().add(
                            new PartitionProduceResponse()
                                    .setIndex(pd.index())
                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
                }
                errorResponse.responses().add(topicResponse);
            }
        }
        return errorResponse;
    }

    private static ProduceRequestData copyEnvelope(ProduceRequestData original) {
        var copy = new ProduceRequestData();
        copy.setAcks(original.acks());
        copy.setTimeoutMs(original.timeoutMs());
        copy.setTransactionalId(original.transactionalId());
        return copy;
    }
}
