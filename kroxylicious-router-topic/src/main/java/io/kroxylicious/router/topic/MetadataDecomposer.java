/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.router.topic;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBrokerCollection;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopicCollection;

/**
 * Splits a METADATA request by topic ownership and merges the
 * per-route responses. Handles the three request variants: all-topics
 * (null), broker-info-only (empty), and specific topics.
 */
class MetadataDecomposer {

    static final MetadataDecomposer INSTANCE = new MetadataDecomposer();

    private MetadataDecomposer() {
    }

    /**
     * Splits a METADATA request into per-route sub-requests.
     *
     * <ul>
     *   <li>{@code topics() == null} (all-topics): one entry per route,
     *       each receiving the full request</li>
     *   <li>{@code topics()} empty (broker-info-only): one entry per route
     *       so that broker addresses from all clusters are discovered</li>
     *   <li>specific topics: grouped by {@code table.routeForTopic()}</li>
     * </ul>
     *
     * @return per-route sub-requests keyed by route name
     */
    Map<String, MetadataRequestData> decompose(MetadataRequestData request,
                                               TopicRoutingTable table,
                                               String defaultRoute) {
        var result = new LinkedHashMap<String, MetadataRequestData>();

        if (request.topics() == null) {
            for (String route : table.allRoutes()) {
                result.put(route, request.duplicate());
            }
            return result;
        }

        if (request.topics().isEmpty()) {
            for (String route : table.allRoutes()) {
                result.put(route, request.duplicate());
            }
            return result;
        }

        for (var topic : request.topics()) {
            String route = table.routeForTopic(topic.name());
            if (route != null) {
                result.computeIfAbsent(route, k -> copyEnvelope(request))
                        .topics().add(topic.duplicate());
            }
        }
        return result;
    }

    /**
     * Merges per-route METADATA responses into a single response.
     *
     * <p>Broker lists are unioned by {@code nodeId}. For all-topics requests,
     * each route's topics are filtered to only include topics that route owns
     * (preventing phantom topics from auto-creation on the wrong cluster).
     * Cluster-level metadata comes from the default route.</p>
     *
     * @param responses per-route responses
     * @param originalRequest the original client request (used to detect all-topics)
     * @param table the router table (used for phantom filtering)
     * @param defaultRoute the default route name
     * @return the merged response
     */
    MetadataResponseData recompose(Map<String, MetadataResponseData> responses,
                                   MetadataRequestData originalRequest,
                                   TopicRoutingTable table,
                                   String defaultRoute) {
        var merged = new MetadataResponseData();

        MetadataResponseData baseResponse = responses.get(defaultRoute);
        if (baseResponse == null) {
            baseResponse = responses.values().iterator().next();
        }
        merged.setClusterId(baseResponse.clusterId());
        merged.setControllerId(baseResponse.controllerId());
        merged.setClusterAuthorizedOperations(baseResponse.clusterAuthorizedOperations());

        merged.setBrokers(unionBrokers(responses, defaultRoute));
        merged.setTopics(mergeTopics(responses, originalRequest, table));

        int maxThrottle = 0;
        for (var resp : responses.values()) {
            maxThrottle = Math.max(maxThrottle, resp.throttleTimeMs());
        }
        merged.setThrottleTimeMs(maxThrottle);

        return merged;
    }

    private static MetadataResponseBrokerCollection unionBrokers(
                                                                 Map<String, MetadataResponseData> responses,
                                                                 String defaultRoute) {
        var result = new MetadataResponseBrokerCollection();
        MetadataResponseData defaultResp = responses.get(defaultRoute);
        if (defaultResp != null) {
            for (var broker : defaultResp.brokers()) {
                result.add(broker.duplicate());
            }
        }
        for (var entry : responses.entrySet()) {
            if (entry.getKey().equals(defaultRoute)) {
                continue;
            }
            for (var broker : entry.getValue().brokers()) {
                if (result.find(broker.nodeId()) == null) {
                    result.add(broker.duplicate());
                }
            }
        }
        return result;
    }

    private static MetadataResponseTopicCollection mergeTopics(
                                                               Map<String, MetadataResponseData> responses,
                                                               MetadataRequestData originalRequest,
                                                               TopicRoutingTable table) {
        var result = new MetadataResponseTopicCollection();
        boolean allTopics = originalRequest.topics() == null;

        for (var entry : responses.entrySet()) {
            String route = entry.getKey();
            for (MetadataResponseTopic topic : entry.getValue().topics()) {
                if (allTopics && topic.name() != null) {
                    String owningRoute = table.routeForTopic(topic.name());
                    if (!route.equals(owningRoute)) {
                        continue;
                    }
                }
                result.add(topic.duplicate());
            }
        }
        return result;
    }

    private static MetadataRequestData copyEnvelope(MetadataRequestData original) {
        var copy = new MetadataRequestData();
        copy.setAllowAutoTopicCreation(original.allowAutoTopicCreation());
        copy.setIncludeClusterAuthorizedOperations(original.includeClusterAuthorizedOperations());
        copy.setIncludeTopicAuthorizedOperations(original.includeTopicAuthorizedOperations());
        return copy;
    }
}
