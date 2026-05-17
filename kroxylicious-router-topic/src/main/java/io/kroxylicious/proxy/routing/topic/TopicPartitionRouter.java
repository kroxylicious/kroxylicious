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

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.kafka.transform.ApiVersionsResponseTransformer;
import io.kroxylicious.kafka.transform.ApiVersionsResponseTransformers;
import io.kroxylicious.proxy.routing.Response;
import io.kroxylicious.proxy.routing.Router;
import io.kroxylicious.proxy.routing.RoutingContext;
import io.kroxylicious.proxy.routing.RoutingResult;
import io.kroxylicious.proxy.routing.topic.ProducerIdManager.ProducerIdEpoch;

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
            ApiKeys.PRODUCE,
            ApiKeys.INIT_PRODUCER_ID,
            ApiKeys.METADATA,
            ApiKeys.FETCH,
            ApiKeys.LIST_OFFSETS,
            ApiKeys.OFFSET_COMMIT);

    private final PrefixTopicRoutingTable routingTable;
    private final String defaultRoute;
    private final Map<ApiKeys, String> staticRoutes;
    private final ApiVersionsResponseTransformer versionCapper;
    private final ProduceDecomposer produceDecomposer = ProduceDecomposer.INSTANCE;
    private final MetadataDecomposer metadataDecomposer = MetadataDecomposer.INSTANCE;
    private final FetchDecomposer fetchDecomposer = FetchDecomposer.INSTANCE;
    private final ListOffsetsDecomposer listOffsetsDecomposer = ListOffsetsDecomposer.INSTANCE;
    private final OffsetCommitDecomposer offsetCommitDecomposer = OffsetCommitDecomposer.INSTANCE;
    private final ProducerIdManager producerIdManager;
    private final FetchSessionManager fetchSessionManager;

    /**
     * @param routingTable determines which route owns each topic
     * @param defaultRoute route used for topics that match no prefix and for non-PRODUCE API keys
     * @param producerIdManager shared manager for per-route producer ID mappings; must outlive
     *                          individual connections so that reconnecting producers retain
     *                          their per-route mappings
     * @param fetchSessionCache shared cache bounding the total number of client-side fetch sessions
     */
    TopicPartitionRouter(PrefixTopicRoutingTable routingTable,
                         String defaultRoute,
                         ProducerIdManager producerIdManager,
                         FetchSessionCache fetchSessionCache) {
        this.routingTable = routingTable;
        this.defaultRoute = defaultRoute;
        this.producerIdManager = producerIdManager;
        this.fetchSessionManager = new FetchSessionManager(fetchSessionCache);
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
        if (apiKey == ApiKeys.INIT_PRODUCER_ID) {
            return handleInitProducerId(header, (InitProducerIdRequestData) request, context);
        }
        if (apiKey == ApiKeys.METADATA) {
            return handleMetadata(header, (MetadataRequestData) request, context);
        }
        if (apiKey == ApiKeys.FETCH) {
            return handleFetch(apiVersion, header, (FetchRequestData) request, context);
        }
        if (apiKey == ApiKeys.LIST_OFFSETS) {
            return handleListOffsets(header, (ListOffsetsRequestData) request, context);
        }
        if (apiKey == ApiKeys.OFFSET_COMMIT) {
            return handleOffsetCommit(header, (OffsetCommitRequestData) request, context);
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
        if (!rewriteProducerIdsForRoutes(subRequests)) {
            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .log("Producer ID mapping not found, returning UNKNOWN_PRODUCER_ID");
            sendProduceResponse(context, unknownProducerIdResponse(request));
            return CompletableFuture.completedFuture(RoutingResult.completed());
        }

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

    private CompletionStage<RoutingResult> handleInitProducerId(
                                                                RequestHeaderData header,
                                                                InitProducerIdRequestData request,
                                                                RoutingContext context) {
        Set<String> allRoutes = routingTable.allRoutes();

        if (allRoutes.size() == 1) {
            return context.sendRequest(defaultRoute, header, request)
                    .thenApply(response -> {
                        context.sendResponse(response);
                        return RoutingResult.completed();
                    });
        }

        Map<String, CompletionStage<Response>> futures = new HashMap<>();
        for (String route : allRoutes) {
            InitProducerIdRequestData routeRequest = rewriteInitProducerIdRequest(request, route);
            futures.put(route, context.sendRequest(route, header, routeRequest));
        }

        return collectAll(futures).thenApply(responses -> {
            Map<String, ProducerIdEpoch> routeMapping = new HashMap<>();
            InitProducerIdResponseData defaultResponse = null;

            for (var entry : responses.entrySet()) {
                var resp = (InitProducerIdResponseData) entry.getValue().body();
                if (resp.errorCode() != Errors.NONE.code()) {
                    LOGGER.atDebug()
                            .addKeyValue("sessionId", context.sessionId())
                            .addKeyValue("route", entry.getKey())
                            .addKeyValue("errorCode", Errors.forCode(resp.errorCode()))
                            .log("INIT_PRODUCER_ID failed on route");
                    context.sendResponse(entry.getValue());
                    return RoutingResult.completed();
                }
                routeMapping.put(entry.getKey(),
                        new ProducerIdEpoch(resp.producerId(), resp.producerEpoch()));
                if (entry.getKey().equals(defaultRoute)) {
                    defaultResponse = resp;
                }
            }

            if (defaultResponse == null) {
                defaultResponse = (InitProducerIdResponseData) responses.values().iterator().next().body();
            }
            producerIdManager.put(defaultResponse.producerId(), routeMapping);

            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("clientProducerId", defaultResponse.producerId())
                    .addKeyValue("routeCount", routeMapping.size())
                    .log("Producer ID mapping established");

            var responseHeader = new ResponseHeaderData().setCorrelationId(0);
            context.sendResponse(new SimpleResponse(responseHeader, defaultResponse));
            return RoutingResult.completed();
        });
    }

    private InitProducerIdRequestData rewriteInitProducerIdRequest(InitProducerIdRequestData original,
                                                                   String route) {
        if (original.producerId() == RecordBatch.NO_PRODUCER_ID || route.equals(defaultRoute)) {
            return original;
        }
        Map<String, ProducerIdEpoch> existing = producerIdManager.get(original.producerId());
        if (existing == null) {
            return original;
        }
        ProducerIdEpoch routeIds = existing.get(route);
        if (routeIds == null) {
            return original;
        }
        return new InitProducerIdRequestData()
                .setTransactionalId(original.transactionalId())
                .setTransactionTimeoutMs(original.transactionTimeoutMs())
                .setProducerId(routeIds.producerId())
                .setProducerEpoch(routeIds.producerEpoch());
    }

    /**
     * @return true if all mappings were found, false if a non-default route had a missing mapping
     */
    private boolean rewriteProducerIdsForRoutes(Map<String, ProduceRequestData> subRequests) {
        for (var entry : subRequests.entrySet()) {
            String route = entry.getKey();
            if (route.equals(defaultRoute)) {
                continue;
            }
            ProduceRequestData subReq = entry.getValue();
            Long clientProducerId = findProducerIdInRequest(subReq);
            if (clientProducerId == null) {
                continue;
            }
            Map<String, ProducerIdEpoch> mapping = producerIdManager.get(clientProducerId);
            if (mapping == null) {
                return false;
            }
            ProducerIdEpoch target = mapping.get(route);
            if (target != null) {
                RecordBatchRewriter.rewriteProducerId(subReq, target.producerId(), target.producerEpoch());
            }
        }
        return true;
    }

    @edu.umd.cs.findbugs.annotations.Nullable
    private static Long findProducerIdInRequest(ProduceRequestData request) {
        for (var td : request.topicData()) {
            for (var pd : td.partitionData()) {
                if (pd.records() == null) {
                    continue;
                }
                MemoryRecords records = (MemoryRecords) pd.records();
                for (RecordBatch batch : records.batches()) {
                    if (batch.producerId() != RecordBatch.NO_PRODUCER_ID) {
                        return batch.producerId();
                    }
                }
            }
        }
        return null;
    }

    private static ProduceResponseData unknownProducerIdResponse(ProduceRequestData request) {
        var response = new ProduceResponseData();
        for (var td : request.topicData()) {
            var topicResponse = new TopicProduceResponse().setName(td.name());
            for (var pd : td.partitionData()) {
                topicResponse.partitionResponses().add(
                        new PartitionProduceResponse()
                                .setIndex(pd.index())
                                .setErrorCode(Errors.UNKNOWN_PRODUCER_ID.code()));
            }
            response.responses().add(topicResponse);
        }
        return response;
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

    private CompletionStage<RoutingResult> handleMetadata(
                                                          RequestHeaderData header,
                                                          MetadataRequestData request,
                                                          RoutingContext context) {
        Map<String, MetadataRequestData> subRequests = metadataDecomposer.decompose(
                request, routingTable, defaultRoute);

        if (subRequests.size() == 1 && request.topics() != null) {
            var entry = subRequests.entrySet().iterator().next();
            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("route", entry.getKey())
                    .log("Metadata routed to single cluster");

            return context.sendRequest(entry.getKey(), header, entry.getValue())
                    .thenApply(response -> {
                        logMergedMetadata(context, (MetadataResponseData) response.body());
                        context.sendResponse(response);
                        return RoutingResult.completed();
                    });
        }

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("routeCount", subRequests.size())
                .log("Metadata fanning out across clusters");

        Map<String, CompletionStage<Response>> futures = new HashMap<>();
        for (var entry : subRequests.entrySet()) {
            futures.put(entry.getKey(),
                    context.sendRequest(entry.getKey(), header, entry.getValue()));
        }

        return collectAll(futures).thenApply(responses -> {
            Map<String, MetadataResponseData> bodies = new HashMap<>();
            for (var entry : responses.entrySet()) {
                bodies.put(entry.getKey(), (MetadataResponseData) entry.getValue().body());
            }
            MetadataResponseData merged = metadataDecomposer.recompose(
                    bodies, request, routingTable, defaultRoute);
            logMergedMetadata(context, merged);
            sendMetadataResponse(context, merged);
            return RoutingResult.completed();
        });
    }

    private static void logMergedMetadata(RoutingContext context,
                                          MetadataResponseData merged) {
        if (LOGGER.isDebugEnabled()) {
            var topicSummary = new java.util.ArrayList<String>();
            for (var topic : merged.topics()) {
                short errorCode = topic.errorCode();
                if (errorCode != 0) {
                    topicSummary.add(topic.name() + "(error=" + Errors.forCode(errorCode) + ")");
                }
                else {
                    topicSummary.add(topic.name() + "(partitions=" + topic.partitions().size() + ")");
                }
            }
            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("brokerCount", merged.brokers().size())
                    .addKeyValue("topicCount", merged.topics().size())
                    .addKeyValue("topics", topicSummary)
                    .addKeyValue("clusterId", merged.clusterId())
                    .log("Merged metadata response");
        }
    }

    private CompletionStage<RoutingResult> handleFetch(
                                                       short apiVersion,
                                                       RequestHeaderData header,
                                                       FetchRequestData request,
                                                       RoutingContext context) {
        var clientResult = fetchSessionManager.processClientRequest(request, apiVersion);
        if (clientResult instanceof FetchSessionManager.ClientRequestResult.SessionError error) {
            sendSyntheticResponse(context, error.response());
            return CompletableFuture.completedFuture(RoutingResult.completed());
        }
        var fullRequest = ((FetchSessionManager.ClientRequestResult.FullFetch) clientResult).request();

        FetchResponseData errorResponse = FetchDecomposer.errorResponseForUnroutableTopics(
                fullRequest, routingTable);
        Map<String, FetchRequestData> subRequests = fetchDecomposer.decompose(
                fullRequest, routingTable);

        if (subRequests.isEmpty()) {
            var clientResponse = fetchSessionManager.computeClientResponse(errorResponse);
            sendSyntheticResponse(context, clientResponse);
            return CompletableFuture.completedFuture(RoutingResult.completed());
        }

        fetchSessionManager.wrapForBackends(subRequests);

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("routeCount", subRequests.size())
                .log("Fetch fanning out across clusters");

        Map<String, CompletionStage<Response>> futures = new HashMap<>();
        for (var entry : subRequests.entrySet()) {
            futures.put(entry.getKey(),
                    context.sendRequest(entry.getKey(), header, entry.getValue()));
        }

        FetchResponseData capturedErrors = errorResponse;
        return collectAll(futures).thenApply(responses -> {
            Map<String, FetchResponseData> bodies = new HashMap<>();
            for (var entry : responses.entrySet()) {
                bodies.put(entry.getKey(), (FetchResponseData) entry.getValue().body());
            }
            fetchSessionManager.processServerResponses(bodies);
            FetchResponseData merged = fetchDecomposer.recompose(bodies, fullRequest);
            for (var tr : capturedErrors.responses()) {
                merged.responses().add(tr.duplicate());
            }
            var clientResponse = fetchSessionManager.computeClientResponse(merged);
            sendSyntheticResponse(context, clientResponse);
            return RoutingResult.completed();
        });
    }

    private CompletionStage<RoutingResult> handleListOffsets(
                                                             RequestHeaderData header,
                                                             ListOffsetsRequestData request,
                                                             RoutingContext context) {
        ListOffsetsResponseData errorResponse = ListOffsetsDecomposer.errorResponseForUnroutableTopics(
                request, routingTable);
        Map<String, ListOffsetsRequestData> subRequests = listOffsetsDecomposer.decompose(
                request, routingTable);

        if (subRequests.isEmpty()) {
            sendSyntheticResponse(context, errorResponse);
            return CompletableFuture.completedFuture(RoutingResult.completed());
        }

        if (subRequests.size() == 1 && errorResponse.topics().isEmpty()) {
            var entry = subRequests.entrySet().iterator().next();
            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("route", entry.getKey())
                    .log("ListOffsets routed to single cluster");
            return context.sendRequest(entry.getKey(), header, entry.getValue())
                    .thenApply(response -> {
                        context.sendResponse(response);
                        return RoutingResult.completed();
                    });
        }

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("routeCount", subRequests.size())
                .log("ListOffsets fanning out across clusters");

        Map<String, CompletionStage<Response>> futures = new HashMap<>();
        for (var entry : subRequests.entrySet()) {
            futures.put(entry.getKey(),
                    context.sendRequest(entry.getKey(), header, entry.getValue()));
        }

        ListOffsetsResponseData capturedErrors = errorResponse;
        return collectAll(futures).thenApply(responses -> {
            Map<String, ListOffsetsResponseData> bodies = new HashMap<>();
            for (var entry : responses.entrySet()) {
                bodies.put(entry.getKey(), (ListOffsetsResponseData) entry.getValue().body());
            }
            ListOffsetsResponseData merged = listOffsetsDecomposer.recompose(bodies, request);
            for (var tr : capturedErrors.topics()) {
                merged.topics().add(tr.duplicate());
            }
            sendSyntheticResponse(context, merged);
            return RoutingResult.completed();
        });
    }

    private CompletionStage<RoutingResult> handleOffsetCommit(
                                                              RequestHeaderData header,
                                                              OffsetCommitRequestData request,
                                                              RoutingContext context) {
        OffsetCommitResponseData errorResponse = OffsetCommitDecomposer.errorResponseForUnroutableTopics(
                request, routingTable);
        Map<String, OffsetCommitRequestData> subRequests = offsetCommitDecomposer.decompose(
                request, routingTable);

        if (subRequests.isEmpty()) {
            sendSyntheticResponse(context, errorResponse);
            return CompletableFuture.completedFuture(RoutingResult.completed());
        }

        if (subRequests.size() == 1 && errorResponse.topics().isEmpty()) {
            var entry = subRequests.entrySet().iterator().next();
            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("route", entry.getKey())
                    .log("OffsetCommit routed to single cluster");
            return context.sendRequest(entry.getKey(), header, entry.getValue())
                    .thenApply(response -> {
                        context.sendResponse(response);
                        return RoutingResult.completed();
                    });
        }

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("routeCount", subRequests.size())
                .log("OffsetCommit fanning out across clusters");

        Map<String, CompletionStage<Response>> futures = new HashMap<>();
        for (var entry : subRequests.entrySet()) {
            futures.put(entry.getKey(),
                    context.sendRequest(entry.getKey(), header, entry.getValue()));
        }

        OffsetCommitResponseData capturedErrors = errorResponse;
        return collectAll(futures).thenApply(responses -> {
            Map<String, OffsetCommitResponseData> bodies = new HashMap<>();
            for (var entry : responses.entrySet()) {
                bodies.put(entry.getKey(), (OffsetCommitResponseData) entry.getValue().body());
            }
            OffsetCommitResponseData merged = offsetCommitDecomposer.recompose(bodies, request);
            for (var tr : capturedErrors.topics()) {
                merged.topics().add(tr.duplicate());
            }
            sendSyntheticResponse(context, merged);
            return RoutingResult.completed();
        });
    }

    private void sendSyntheticResponse(RoutingContext context,
                                       ApiMessage body) {
        var responseHeader = new ResponseHeaderData()
                .setCorrelationId(0);
        context.sendResponse(new SimpleResponse(responseHeader, body));
    }

    private void sendMetadataResponse(RoutingContext context,
                                      MetadataResponseData body) {
        sendSyntheticResponse(context, body);
    }

    private void sendProduceResponse(RoutingContext context,
                                     ProduceResponseData body) {
        sendSyntheticResponse(context, body);
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
