/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.kafka.transform.ApiVersionsResponseTransformer;
import io.kroxylicious.kafka.transform.ApiVersionsResponseTransformers;
import io.kroxylicious.proxy.routing.Router;
import io.kroxylicious.proxy.routing.RoutingContext;
import io.kroxylicious.proxy.routing.RoutingResult;

/**
 * Routes Kafka requests to backend clusters based on topic name
 * ownership. In Phase 1, only API_VERSIONS is dynamically handled
 * (for version capping); all other APIs are statically routed to
 * the default route.
 */
class TopicPartitionRouter implements Router {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionRouter.class);

    /**
     * API keys whose wire format transitions from topic names to topic IDs
     * at certain versions. We cap these to force name-based addressing.
     */
    static final Map<ApiKeys, Short> VERSION_CAPS = Map.of(
            ApiKeys.PRODUCE, (short) 12,
            ApiKeys.FETCH, (short) 12,
            ApiKeys.OFFSET_COMMIT, (short) 9,
            ApiKeys.OFFSET_FETCH, (short) 9,
            ApiKeys.DELETE_TOPICS, (short) 5);

    /**
     * API keys that require dynamic routing (topic inspection or
     * response transformation). Everything else is statically routed.
     */
    private static final Set<ApiKeys> DYNAMICALLY_ROUTED = Set.of(ApiKeys.API_VERSIONS);

    private final PrefixTopicRoutingTable routingTable;
    private final String defaultRoute;
    private final Map<ApiKeys, String> staticRoutes;
    private final ApiVersionsResponseTransformer versionCapper;

    TopicPartitionRouter(PrefixTopicRoutingTable routingTable,
                         String defaultRoute) {
        this.routingTable = routingTable;
        this.defaultRoute = defaultRoute;
        this.staticRoutes = Arrays.stream(ApiKeys.values())
                .filter(k -> !DYNAMICALLY_ROUTED.contains(k))
                .collect(Collectors.toUnmodifiableMap(k -> k, k -> defaultRoute));
        this.versionCapper = ApiVersionsResponseTransformers.limitMaxVersionForApiKeys(VERSION_CAPS);
    }

    @Override
    public Map<ApiKeys, String> staticRoutes() {
        return staticRoutes;
    }

    @Override
    public CompletionStage<RoutingResult> onClientRequest(
                                                          short apiVersion,
                                                          ApiKeys apiKey,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          RoutingContext context) {
        if (apiKey == ApiKeys.API_VERSIONS) {
            return handleApiVersions(apiVersion, header, request, context);
        }

        return context.sendRequest(defaultRoute, header, request)
                .thenApply(response -> {
                    context.sendResponse(response);
                    return RoutingResult.completed();
                });
    }

    private CompletionStage<RoutingResult> handleApiVersions(
                                                             short apiVersion,
                                                             RequestHeaderData header,
                                                             ApiMessage request,
                                                             RoutingContext context) {
        return context.sendRequest(defaultRoute, header, request)
                .thenApply(response -> {
                    versionCapper.transform(
                            (org.apache.kafka.common.message.ApiVersionsResponseData) response.body());
                    LOGGER.atDebug()
                            .addKeyValue("sessionId", context.sessionId())
                            .addKeyValue("versionCaps", VERSION_CAPS)
                            .log("Capped API versions for topic-ID-bearing API keys");
                    context.sendResponse(response);
                    return RoutingResult.completed();
                });
    }
}
