/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.kafka.transform.ApiVersionsResponseTransformer;
import io.kroxylicious.kafka.transform.ApiVersionsResponseTransformers;
import io.kroxylicious.proxy.routing.Response;
import io.kroxylicious.proxy.routing.Router;
import io.kroxylicious.proxy.routing.RoutingContext;
import io.kroxylicious.proxy.routing.RoutingResult;

/**
 * Routes Kafka requests to backend clusters based on topic name
 * ownership. API_VERSIONS is intercepted for version capping.
 * PRODUCE requests are decomposed by topic and fanned out to the
 * owning clusters.
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

    private static final Set<ApiKeys> DYNAMICALLY_ROUTED = Set.of(
            ApiKeys.API_VERSIONS,
            ApiKeys.PRODUCE);

    private final PrefixTopicRoutingTable routingTable;
    private final String defaultRoute;
    private final Map<ApiKeys, String> staticRoutes;
    private final ApiVersionsResponseTransformer versionCapper;
    private final ProduceDecomposer produceDecomposer = ProduceDecomposer.INSTANCE;

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
            return handleApiVersions(header, request, context);
        }
        if (apiKey == ApiKeys.PRODUCE) {
            return handleProduce(header, (ProduceRequestData) request, context);
        }

        return context.sendRequest(defaultRoute, header, request)
                .thenApply(response -> {
                    context.sendResponse(response);
                    return RoutingResult.completed();
                });
    }

    private CompletionStage<RoutingResult> handleApiVersions(
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

    private CompletionStage<RoutingResult> handleProduce(
                                                         RequestHeaderData header,
                                                         ProduceRequestData request,
                                                         RoutingContext context) {
        ProduceResponseData errorResponse = ProduceDecomposer.errorResponseForUnroutableTopics(
                request, routingTable);
        Map<String, ProduceRequestData> subRequests = produceDecomposer.decompose(
                request, routingTable);

        boolean isAcksZero = request.acks() == 0;

        if (subRequests.isEmpty()) {
            sendProduceResponse(context, errorResponse);
            return CompletableFuture.completedFuture(RoutingResult.completed());
        }

        if (subRequests.size() == 1 && errorResponse.responses().isEmpty()) {
            var entry = subRequests.entrySet().iterator().next();
            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("route", entry.getKey())
                    .log("Produce routed to single cluster");

            if (isAcksZero) {
                context.sendRequest(entry.getKey(), header, entry.getValue());
                sendProduceResponse(context, new ProduceResponseData());
                return CompletableFuture.completedFuture(RoutingResult.completed());
            }

            return context.sendRequest(entry.getKey(), header, entry.getValue())
                    .thenApply(response -> {
                        context.sendResponse(response);
                        return RoutingResult.completed();
                    });
        }

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("routeCount", subRequests.size())
                .log("Produce fanning out across clusters");

        if (isAcksZero) {
            for (var entry : subRequests.entrySet()) {
                context.sendRequest(entry.getKey(), header, entry.getValue());
            }
            sendProduceResponse(context, mergeWithErrors(Map.of(), errorResponse, request));
            return CompletableFuture.completedFuture(RoutingResult.completed());
        }

        Map<String, CompletionStage<Response>> futures = new HashMap<>();
        for (var entry : subRequests.entrySet()) {
            futures.put(entry.getKey(),
                    context.sendRequest(entry.getKey(), header, entry.getValue()));
        }

        ProduceResponseData capturedErrors = errorResponse;
        return collectAll(futures).thenApply(responses -> {
            Map<String, ProduceResponseData> bodies = new HashMap<>();
            for (var entry : responses.entrySet()) {
                bodies.put(entry.getKey(), (ProduceResponseData) entry.getValue().body());
            }
            ProduceResponseData merged = mergeWithErrors(bodies, capturedErrors, request);
            sendProduceResponse(context, merged);
            return RoutingResult.completed();
        });
    }

    private ProduceResponseData mergeWithErrors(Map<String, ProduceResponseData> routeResponses,
                                                ProduceResponseData errorResponse,
                                                ProduceRequestData originalRequest) {
        ProduceResponseData merged = produceDecomposer.recompose(routeResponses, originalRequest);
        for (var tr : errorResponse.responses()) {
            merged.responses().add(tr.duplicate());
        }
        return merged;
    }

    private void sendProduceResponse(RoutingContext context,
                                     ProduceResponseData body) {
        var responseHeader = new ResponseHeaderData()
                .setCorrelationId(0);
        context.sendResponse(new SimpleResponse(responseHeader, body));
    }

    private static <K> CompletionStage<Map<K, Response>> collectAll(
                                                                    Map<K, CompletionStage<Response>> futures) {
        Map<K, Response> results = new HashMap<>();
        CompletableFuture<Map<K, Response>> combined = CompletableFuture.completedFuture(results);
        for (var entry : futures.entrySet()) {
            combined = combined.thenCombine(entry.getValue(), (map, response) -> {
                map.put(entry.getKey(), response);
                return map;
            });
        }
        return combined;
    }
}
