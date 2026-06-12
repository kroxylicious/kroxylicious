/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult;
import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;

import io.kroxylicious.kafka.transform.ApiVersionsResponseTransformer;
import io.kroxylicious.kafka.transform.ApiVersionsResponseTransformers;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.RouterResponse;
import io.kroxylicious.proxy.router.topic.ProducerIdManager.ProducerIdEpoch;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Routes Kafka requests to backend clusters based on topic name ownership.
 *
 * <h2>Broker targeting</h2>
 *
 * <p>Each Kafka API must be sent to a specific broker type:</p>
 * <ul>
 *   <li><b>Any broker:</b> API_VERSIONS, METADATA, FIND_COORDINATOR, CREATE_TOPICS,
 *       DELETE_TOPICS, CREATE_PARTITIONS, DESCRIBE_CLUSTER — dispatched via
 *       {@link RouterContext#sendRequestToNode}
 *       to the route's bootstrap.</li>
 *   <li><b>Partition leader:</b> PRODUCE, LIST_OFFSETS, DELETE_RECORDS — dispatched
 *       via {@link RouterContext#sendRequestToNode} to the cached partition leader.
 *       FETCH is dispatched per-route (not per-leader) due to fetch session
 *       management constraints.</li>
 *   <li><b>Group coordinator:</b> OFFSET_COMMIT, OFFSET_FETCH,
 *       CONSUMER_GROUP_HEARTBEAT, CONSUMER_GROUP_DESCRIBE — dispatched via
 *       {@code sendRequestToNode} to the group coordinator discovered by
 *       FIND_COORDINATOR.</li>
 *   <li><b>Transaction coordinator:</b> INIT_PRODUCER_ID, ADD_PARTITIONS_TO_TXN,
 *       ADD_OFFSETS_TO_TXN, END_TXN, TXN_OFFSET_COMMIT — dispatched via
 *       {@code sendRequestToNode} to the transaction coordinator.</li>
 * </ul>
 *
 * <h2>Leader/coordinator cache</h2>
 *
 * <p>The router maintains a per-topic-partition leader cache ({@code partitionLeaders})
 * populated from every METADATA response that passes through. When a partition-leader
 * API arrives and the leader is not yet cached, the router sends an internal METADATA
 * request to the relevant route before dispatching.</p>
 *
 * <h2>Staleness handling</h2>
 *
 * <p>When a backend returns {@code NOT_LEADER_OR_FOLLOWER} or {@code NOT_COORDINATOR},
 * the router:</p>
 * <ol>
 *   <li>Returns the original error to the client <b>unchanged</b> — the client
 *       needs it to trigger its own metadata refresh.</li>
 *   <li>Fires a background METADATA (or FIND_COORDINATOR) request to refresh the
 *       cache for subsequent requests.</li>
 * </ol>
 *
 * <p>Suppressing the error and retrying silently would leave the client's metadata
 * stale, causing an infinite retry loop. Returning the error lets the Kafka
 * protocol's built-in metadata refresh work as designed. This also matters in
 * multi-proxy deployments where the client may reconnect to a different instance.</p>
 */
class TopicPartitionRouter implements Router {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionRouter.class);

    /**
     * API keys whose wire format changes structurally at certain versions.
     * TopicId-bearing APIs (PRODUCE, FETCH, OFFSET_COMMIT, OFFSET_FETCH,
     * DELETE_TOPICS) are handled by the router's topicId cache — no cap needed.
     */
    static final Map<ApiKeys, Short> VERSION_CAPS = Map.of(
            ApiKeys.ADD_PARTITIONS_TO_TXN, (short) 3,
            ApiKeys.FIND_COORDINATOR, (short) 3);

    private static final Set<ApiKeys> DYNAMICALLY_ROUTED = Set.of(
            ApiKeys.API_VERSIONS,
            ApiKeys.PRODUCE,
            ApiKeys.INIT_PRODUCER_ID,
            ApiKeys.METADATA,
            ApiKeys.FETCH,
            ApiKeys.LIST_OFFSETS,
            ApiKeys.OFFSET_FOR_LEADER_EPOCH,
            ApiKeys.OFFSET_COMMIT,
            ApiKeys.OFFSET_FETCH,
            ApiKeys.CREATE_TOPICS,
            ApiKeys.DELETE_TOPICS,
            ApiKeys.CREATE_PARTITIONS,
            ApiKeys.DELETE_RECORDS,
            ApiKeys.FIND_COORDINATOR,
            ApiKeys.DESCRIBE_CLUSTER,
            ApiKeys.ADD_PARTITIONS_TO_TXN,
            ApiKeys.ADD_OFFSETS_TO_TXN,
            ApiKeys.TXN_OFFSET_COMMIT,
            ApiKeys.END_TXN,
            ApiKeys.CONSUMER_GROUP_HEARTBEAT,
            ApiKeys.CONSUMER_GROUP_DESCRIBE);

    private static final Set<ApiKeys> SUBJECT_ROUTED_API_KEYS = Set.of(
            ApiKeys.FIND_COORDINATOR,
            ApiKeys.INIT_PRODUCER_ID,
            ApiKeys.ADD_PARTITIONS_TO_TXN,
            ApiKeys.ADD_OFFSETS_TO_TXN,
            ApiKeys.TXN_OFFSET_COMMIT,
            ApiKeys.END_TXN,
            ApiKeys.OFFSET_COMMIT,
            ApiKeys.OFFSET_FETCH,
            ApiKeys.CONSUMER_GROUP_HEARTBEAT,
            ApiKeys.CONSUMER_GROUP_DESCRIBE);

    static final String REJECTED_ASSIGNMENTS_METRIC = "kroxylicious_routing_rejected_assignments_total";
    static final String VIRTUAL_CLUSTER_TAG = "virtual_cluster";
    static final String ROUTER_TAG = "router";

    private final PrefixTopicRoutingTable routingTable;
    private final String defaultRoute;
    private final Map<ApiKeys, String> staticRoutes;
    private final ApiVersionsResponseTransformer versionCapper;
    private final ProduceDecomposer produceDecomposer = ProduceDecomposer.INSTANCE;
    private final MetadataDecomposer metadataDecomposer = MetadataDecomposer.INSTANCE;
    private final FetchDecomposer fetchDecomposer = FetchDecomposer.INSTANCE;
    private final ListOffsetsDecomposer listOffsetsDecomposer = ListOffsetsDecomposer.INSTANCE;
    private final OffsetCommitDecomposer offsetCommitDecomposer = OffsetCommitDecomposer.INSTANCE;
    private final OffsetFetchDecomposer offsetFetchDecomposer = OffsetFetchDecomposer.INSTANCE;
    private final CreateTopicsDecomposer createTopicsDecomposer = CreateTopicsDecomposer.INSTANCE;
    private final DeleteTopicsDecomposer deleteTopicsDecomposer = DeleteTopicsDecomposer.INSTANCE;
    private final CreatePartitionsDecomposer createPartitionsDecomposer = CreatePartitionsDecomposer.INSTANCE;
    private final DeleteRecordsDecomposer deleteRecordsDecomposer = DeleteRecordsDecomposer.INSTANCE;
    private final Map<String, String> subjectRoutes;
    private final ProducerIdManager producerIdManager;
    private final FetchSessionManager fetchSessionManager;
    private final Counter rejectedAssignmentsCounter;

    // Coordinator caches for the non-subject-routed path only.
    private final Map<String, Integer> transactionCoordinators = new HashMap<>();
    private final Map<String, Integer> consumerGroupCoordinators = new HashMap<>();
    /**
     * Cached partition leaders: topicName → (partitionIndex → virtualLeaderNodeId).
     * Node IDs are virtual (translated by {@code NodeIdResponseTranslator} before the
     * router sees them). Updated from every METADATA response that passes through
     * the router. Plain {@code HashMap} because the router executes on a single
     * Netty event loop thread (see {@link Router#onRequest} threading contract).
     */
    private final Map<String, Map<Integer, Integer>> partitionLeaders = new HashMap<>();

    @Nullable
    private String activeTransactionRoute;

    private static final short INTERNAL_METADATA_API_VERSION = 12;
    private static final short FIND_COORDINATOR_API_VERSION = 3;

    /**
     * @param routingTable determines which route owns each topic
     * @param defaultRoute route used for topics that match no prefix and for non-PRODUCE API keys
     * @param subjectRoutes mapping from username to route name for subject-routed users
     * @param producerIdManager shared manager for per-route producer ID mappings; must outlive
     *                          individual connections so that reconnecting producers retain
     *                          their per-route mappings
     * @param fetchSessionCache shared cache bounding the total number of client-side fetch sessions
     */
    TopicPartitionRouter(PrefixTopicRoutingTable routingTable,
                         String defaultRoute,
                         Map<String, String> subjectRoutes,
                         ProducerIdManager producerIdManager,
                         FetchSessionCache fetchSessionCache,
                         Clock clock,
                         String virtualClusterName,
                         String routerName) {
        this.routingTable = routingTable;
        this.defaultRoute = defaultRoute;
        this.subjectRoutes = subjectRoutes;
        this.producerIdManager = producerIdManager;
        this.fetchSessionManager = new FetchSessionManager(fetchSessionCache, clock);
        this.staticRoutes = Arrays.stream(ApiKeys.values())
                .filter(k -> !DYNAMICALLY_ROUTED.contains(k))
                .collect(Collectors.toUnmodifiableMap(k -> k, k -> defaultRoute));
        this.versionCapper = ApiVersionsResponseTransformers.limitMaxVersionForApiKeys(VERSION_CAPS);
        this.rejectedAssignmentsCounter = Counter.builder(REJECTED_ASSIGNMENTS_METRIC)
                .description("Number of topics rejected because they specified explicit broker assignments.")
                .tag(VIRTUAL_CLUSTER_TAG, virtualClusterName)
                .tag(ROUTER_TAG, routerName)
                .register(Metrics.globalRegistry);
    }

    @Override
    public void close() {
        fetchSessionManager.close();
        Metrics.globalRegistry.remove(rejectedAssignmentsCounter);
    }

    @Override
    public Map<ApiKeys, String> staticRoutes() {
        return staticRoutes;
    }

    @Override
    public CompletionStage<RouterResponse> onRequest(
                                                     short apiVersion,
                                                     ApiKeys apiKey,
                                                     RequestHeaderData header,
                                                     ApiMessage request,
                                                     RouterContext context) {
        // Subject-routed users have coordinator-bound operations forwarded to their
        // assigned route. Topic-addressed ops (PRODUCE, FETCH, etc.) still go through
        // the normal handlers for leader-based router. METADATA and admin ops fan out.
        String subjectRoute = subjectRouteFor(context.authenticatedSubject());
        if (subjectRoute != null && SUBJECT_ROUTED_API_KEYS.contains(apiKey)) {
            return forwardToRoute(subjectRoute, header, request, context);
        }

        // Non-subject-routed users: topic-addressed requests are decomposed across routes,
        // coordinator-bound requests go to the default route.
        if (apiKey == ApiKeys.API_VERSIONS) {
            return handleApiVersions(header, request, context);
        }
        if (apiKey == ApiKeys.PRODUCE) {
            return handleProduce(apiVersion, header, (ProduceRequestData) request, context);
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
            return handleListOffsets(apiVersion, header, (ListOffsetsRequestData) request, context);
        }
        if (apiKey == ApiKeys.OFFSET_FOR_LEADER_EPOCH) {
            return handleOffsetForLeaderEpoch(header, (OffsetForLeaderEpochRequestData) request, context);
        }
        if (apiKey == ApiKeys.OFFSET_COMMIT) {
            return handleOffsetCommit(apiVersion, header, (OffsetCommitRequestData) request, context);
        }
        if (apiKey == ApiKeys.OFFSET_FETCH) {
            return handleOffsetFetch(apiVersion, header, (OffsetFetchRequestData) request, context);
        }
        if (apiKey == ApiKeys.CREATE_TOPICS) {
            return handleCreateTopics(apiVersion, header, (CreateTopicsRequestData) request, context);
        }
        if (apiKey == ApiKeys.DELETE_TOPICS) {
            return handleDeleteTopics(apiVersion, header, (DeleteTopicsRequestData) request, context);
        }
        if (apiKey == ApiKeys.CREATE_PARTITIONS) {
            return handleCreatePartitions(apiVersion, header, (CreatePartitionsRequestData) request, context);
        }
        if (apiKey == ApiKeys.DELETE_RECORDS) {
            return handleDeleteRecords(apiVersion, header, (DeleteRecordsRequestData) request, context);
        }
        if (apiKey == ApiKeys.ADD_PARTITIONS_TO_TXN) {
            return handleAddPartitionsToTxn(header,
                    (AddPartitionsToTxnRequestData) request, context);
        }
        if (apiKey == ApiKeys.ADD_OFFSETS_TO_TXN) {
            return handleAddOffsetsToTxn(header,
                    (AddOffsetsToTxnRequestData) request, context);
        }
        if (apiKey == ApiKeys.TXN_OFFSET_COMMIT) {
            return handleTxnOffsetCommit(header,
                    (TxnOffsetCommitRequestData) request, context);
        }
        if (apiKey == ApiKeys.END_TXN) {
            return handleEndTxn(header, (EndTxnRequestData) request, context);
        }
        if (apiKey == ApiKeys.FIND_COORDINATOR) {
            return handleFindCoordinator(header, request, context);
        }
        if (apiKey == ApiKeys.CONSUMER_GROUP_HEARTBEAT) {
            return handleConsumerGroupHeartbeat(header,
                    (ConsumerGroupHeartbeatRequestData) request, context);
        }
        if (apiKey == ApiKeys.CONSUMER_GROUP_DESCRIBE) {
            return handleConsumerGroupDescribe(header,
                    (ConsumerGroupDescribeRequestData) request, context);
        }
        if (apiKey == ApiKeys.DESCRIBE_CLUSTER) {
            return handleDescribeCluster(header, request, context);
        }

        return context.sendRequestToNode(context.anyNodeId(defaultRoute), header, request)
                .thenApply(response -> context.respondWith(response).build());
    }

    private CompletionStage<RouterResponse> handleApiVersions(
                                                              RequestHeaderData header,
                                                              ApiMessage request,
                                                              RouterContext context) {
        int targetNode = context.virtualNodeId().orElse(context.anyNodeId(defaultRoute));
        return context.sendRequestToNode(targetNode, header, request)
                .thenApply(response -> {
                    versionCapper.transform(
                            (org.apache.kafka.common.message.ApiVersionsResponseData) response);
                    LOGGER.atDebug()
                            .addKeyValue("sessionId", context.sessionId())
                            .addKeyValue("versionCaps", VERSION_CAPS)
                            .log("Capped API versions for topic-ID-bearing API keys");
                    return context.respondWith(response).build();
                });
    }

    private CompletionStage<RouterResponse> handleProduce(
                                                          short apiVersion,
                                                          RequestHeaderData header,
                                                          ProduceRequestData request,
                                                          RouterContext context) {
        ProduceResponseData errorResponse = ProduceDecomposer.errorResponseForUnroutableTopics(
                request, routingTable, apiVersion);
        Map<String, ProduceRequestData> subRequests = produceDecomposer.decompose(
                request, routingTable, apiVersion, context::topicName);
        boolean subjectRouted = subjectRouteFor(context.authenticatedSubject()) != null;
        if (!subjectRouted && !rewriteProducerIdsForRoutes(subRequests)) {
            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .log("Producer ID mapping not found, returning UNKNOWN_PRODUCER_ID");
            return CompletableFuture.completedFuture(
                    syntheticResult(context,
                            unknownProducerIdResponse(request, apiVersion)));
        }

        boolean isAcksZero = request.acks() == 0;

        if (subRequests.isEmpty()) {
            return CompletableFuture.completedFuture(
                    syntheticResult(context, errorResponse));
        }

        // acks=0: fire-and-forget to route bootstrap
        if (isAcksZero) {
            for (var entry : subRequests.entrySet()) {
                context.sendRequestToNode(context.anyNodeId(entry.getKey()), header, entry.getValue());
            }
            return CompletableFuture.completedFuture(
                    syntheticResult(context,
                            mergeWithErrors(Map.of(), errorResponse,
                                    request, apiVersion)));
        }

        return ensureLeadersCached(subRequests, context).thenCompose(v -> {
            Map<Integer, ProduceRequestData> byLeader = groupProduceByLeader(subRequests, request);

            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("leaderCount", byLeader.size())
                    .log("Produce dispatching to partition leaders");

            Map<Integer, CompletionStage<ApiMessage>> futures = new HashMap<>();
            for (var entry : byLeader.entrySet()) {
                futures.put(entry.getKey(),
                        context.sendRequestToNode(entry.getKey(), header, entry.getValue()));
            }

            ProduceResponseData capturedErrors = errorResponse;
            return collectAll(futures).thenApply(responses -> {
                Map<String, ProduceResponseData> bodies = new HashMap<>();
                for (var entry : responses.entrySet()) {
                    bodies.put(String.valueOf(entry.getKey()),
                            (ProduceResponseData) entry.getValue());
                }
                ProduceResponseData merged = mergeWithErrors(bodies, capturedErrors, request, apiVersion);
                return syntheticResult(context, merged);
            });
        });
    }

    private CompletionStage<RouterResponse> handleInitProducerId(
                                                                 RequestHeaderData header,
                                                                 InitProducerIdRequestData request,
                                                                 RouterContext context) {
        Set<String> allRoutes = routingTable.allRoutes();
        boolean isTransactional = request.transactionalId() != null
                && !request.transactionalId().isEmpty();

        if (allRoutes.size() == 1) {
            return context.sendRequestToNode(context.anyNodeId(defaultRoute), header, request)
                    .thenApply(response -> {
                        return context.respondWith(response).build();
                    });
        }

        if (isTransactional) {
            String txnRoute = defaultRoute;
            return discoverCoordinatorAndInitProducerId(
                    header, request, txnRoute, context);
        }

        return fanOutInitProducerId(header, request, allRoutes, context);
    }

    private CompletionStage<RouterResponse> fanOutInitProducerId(
                                                                 RequestHeaderData header,
                                                                 InitProducerIdRequestData request,
                                                                 Set<String> allRoutes,
                                                                 RouterContext context) {
        Map<String, CompletionStage<ApiMessage>> futures = new HashMap<>();
        for (String route : allRoutes) {
            InitProducerIdRequestData routeRequest = rewriteInitProducerIdRequest(request, route);
            futures.put(route, context.sendRequestToNode(context.anyNodeId(route), header, routeRequest));
        }

        return collectAll(futures).thenApply(responses -> {
            Map<String, ProducerIdEpoch> routeMapping = new HashMap<>();
            InitProducerIdResponseData defaultResponse = null;

            for (var entry : responses.entrySet()) {
                var resp = (InitProducerIdResponseData) entry.getValue();
                if (resp.errorCode() != Errors.NONE.code()) {
                    LOGGER.atDebug()
                            .addKeyValue("sessionId", context.sessionId())
                            .addKeyValue("route", entry.getKey())
                            .addKeyValue("errorCode", Errors.forCode(resp.errorCode()))
                            .log("INIT_PRODUCER_ID failed on route");
                    return context.respondWith(entry.getValue()).build();
                }
                routeMapping.put(entry.getKey(),
                        new ProducerIdEpoch(resp.producerId(), resp.producerEpoch()));
                if (entry.getKey().equals(defaultRoute)) {
                    defaultResponse = resp;
                }
            }

            if (defaultResponse == null) {
                defaultResponse = (InitProducerIdResponseData) responses.values().iterator().next();
            }
            producerIdManager.put(defaultResponse.producerId(), routeMapping);

            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("clientProducerId", defaultResponse.producerId())
                    .addKeyValue("routeCount", routeMapping.size())
                    .log("Producer ID mapping established");

            var responseHeader = new ResponseHeaderData().setCorrelationId(0);
            return context.respondWith(responseHeader, defaultResponse).build();
        });
    }

    private CompletionStage<Integer> discoverCoordinator(
                                                         String route,
                                                         byte keyType,
                                                         String key,
                                                         RouterContext context) {
        var mdHeader = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.METADATA.id)
                .setRequestApiVersion(INTERNAL_METADATA_API_VERSION);
        var mdReq = new MetadataRequestData();

        return context.sendRequestToNode(context.anyNodeId(route), mdHeader, mdReq).thenCompose(mdResponse -> {
            updateLeaderCache((MetadataResponseData) mdResponse);
            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("route", route)
                    .log("Broker discovery completed before coordinator lookup");

            var findCoordHeader = new RequestHeaderData()
                    .setRequestApiKey(ApiKeys.FIND_COORDINATOR.id)
                    .setRequestApiVersion(FIND_COORDINATOR_API_VERSION);
            var findCoordReq = new FindCoordinatorRequestData()
                    .setKey(key)
                    .setKeyType(keyType);

            return context.sendRequestToNode(context.anyNodeId(route), findCoordHeader, findCoordReq);
        }).thenApply(coordResponse -> {
            var resp = (FindCoordinatorResponseData) coordResponse;
            if (resp.errorCode() != Errors.NONE.code()) {
                throw new CoordinatorDiscoveryException(Errors.forCode(resp.errorCode()));
            }
            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("route", route)
                    .addKeyValue("coordinatorNodeId", resp.nodeId())
                    .addKeyValue("key", key)
                    .addKeyValue("keyType", keyType)
                    .log("Discovered coordinator");
            return resp.nodeId();
        });
    }

    private CompletionStage<RouterResponse> discoverCoordinatorAndInitProducerId(
                                                                                 RequestHeaderData header,
                                                                                 InitProducerIdRequestData request,
                                                                                 String txnRoute,
                                                                                 RouterContext context) {
        return discoverCoordinator(txnRoute, (byte) 1, request.transactionalId(), context)
                .thenCompose(coordinatorNodeId -> {
                    transactionCoordinators.put(txnRoute, coordinatorNodeId);

                    var initHeader = new RequestHeaderData()
                            .setRequestApiKey(ApiKeys.INIT_PRODUCER_ID.id)
                            .setRequestApiVersion(header.requestApiVersion());

                    return context.sendRequestToNode(coordinatorNodeId, initHeader, request)
                            .<RouterResponse> thenApply(initResponse -> {
                                var initResp = (InitProducerIdResponseData) initResponse;
                                if (initResp.errorCode() != Errors.NONE.code()) {
                                    LOGGER.atDebug()
                                            .addKeyValue("sessionId", context.sessionId())
                                            .addKeyValue("route", txnRoute)
                                            .addKeyValue("errorCode", Errors.forCode(initResp.errorCode()))
                                            .log("Transactional INIT_PRODUCER_ID failed on route");
                                    return context.respondWith(initResponse).build();
                                }

                                Map<String, ProducerIdEpoch> routeMapping = Map.of(
                                        txnRoute,
                                        new ProducerIdEpoch(initResp.producerId(), initResp.producerEpoch()));
                                producerIdManager.put(initResp.producerId(), routeMapping);

                                LOGGER.atDebug()
                                        .addKeyValue("sessionId", context.sessionId())
                                        .addKeyValue("clientProducerId", initResp.producerId())
                                        .addKeyValue("route", txnRoute)
                                        .addKeyValue("transactionalId", request.transactionalId())
                                        .log("Transactional producer ID mapping established");

                                var responseHeader = new ResponseHeaderData()
                                        .setCorrelationId(0);
                                return context.respondWith(
                                        responseHeader, initResp).build();
                            });
                }).exceptionally(ex -> {
                    LOGGER.atWarn()
                            .addKeyValue("sessionId", context.sessionId())
                            .addKeyValue("route", txnRoute)
                            .addKeyValue("transactionalId", request.transactionalId())
                            .setCause(LOGGER.isDebugEnabled() ? ex : null)
                            .addKeyValue("error", ex.getMessage())
                            .log(LOGGER.isDebugEnabled()
                                    ? "Transactional INIT_PRODUCER_ID failed"
                                    : "Transactional INIT_PRODUCER_ID failed, "
                                            + "increase log level to DEBUG for stacktrace");
                    return syntheticResult(context,
                            new InitProducerIdResponseData()
                                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()));
                });
    }

    static final class CoordinatorDiscoveryException extends RuntimeException {
        private final Errors error;

        CoordinatorDiscoveryException(Errors error) {
            super("Coordinator discovery failed: " + error);
            this.error = error;
        }

        Errors error() {
            return error;
        }
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

    private static ProduceResponseData unknownProducerIdResponse(ProduceRequestData request,
                                                                 short apiVersion) {
        var response = new ProduceResponseData();
        for (var td : request.topicData()) {
            var topicResponse = new TopicProduceResponse();
            if (apiVersion >= 13) {
                topicResponse.setTopicId(td.topicId());
            }
            else {
                topicResponse.setName(td.name());
            }
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
                                                ProduceRequestData originalRequest,
                                                short apiVersion) {
        ProduceResponseData merged = produceDecomposer.recompose(routeResponses, originalRequest, apiVersion);
        for (var tr : errorResponse.responses()) {
            merged.responses().add(tr.duplicate());
        }
        return merged;
    }

    private CompletionStage<RouterResponse> handleMetadata(
                                                           RequestHeaderData header,
                                                           MetadataRequestData request,
                                                           RouterContext context) {
        Map<String, MetadataRequestData> subRequests = metadataDecomposer.decompose(
                request, routingTable, defaultRoute);

        if (subRequests.size() == 1 && request.topics() != null) {
            var entry = subRequests.entrySet().iterator().next();
            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("route", entry.getKey())
                    .log("Metadata routed to single cluster");

            return context.sendRequestToNode(context.anyNodeId(entry.getKey()), header, entry.getValue())
                    .thenApply(response -> {
                        var md = (MetadataResponseData) response;
                        updateLeaderCache(md);
                        logMergedMetadata(context, md);
                        return context.respondWith(response).build();
                    });
        }

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("routeCount", subRequests.size())
                .log("Metadata fanning out across clusters");

        Map<String, CompletionStage<ApiMessage>> futures = new HashMap<>();
        for (var entry : subRequests.entrySet()) {
            futures.put(entry.getKey(),
                    context.sendRequestToNode(context.anyNodeId(entry.getKey()), header, entry.getValue()));
        }

        return collectAll(futures).thenApply(responses -> {
            Map<String, MetadataResponseData> bodies = new HashMap<>();
            for (var entry : responses.entrySet()) {
                var md = (MetadataResponseData) entry.getValue();
                updateLeaderCache(md);
                bodies.put(entry.getKey(), md);
            }
            MetadataResponseData merged = metadataDecomposer.recompose(
                    bodies, request, routingTable, defaultRoute);
            logMergedMetadata(context, merged);
            return syntheticResult(context, merged);
        });
    }

    private static void logMergedMetadata(RouterContext context,
                                          MetadataResponseData merged) {
        if (LOGGER.isDebugEnabled()) {
            var topicSummary = new ArrayList<String>();
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

    private CompletionStage<RouterResponse> handleFetch(
                                                        short apiVersion,
                                                        RequestHeaderData header,
                                                        FetchRequestData request,
                                                        RouterContext context) {
        boolean usesTopicIds = apiVersion >= 13;

        var clientResult = fetchSessionManager.processClientRequest(request, apiVersion);
        if (clientResult instanceof FetchSessionManager.ClientRequestResult.SessionError error) {
            return CompletableFuture.completedFuture(syntheticResult(context, error.response()));
        }
        var fullRequest = ((FetchSessionManager.ClientRequestResult.FullFetch) clientResult).request();

        FetchResponseData errorResponse = FetchDecomposer.errorResponseForUnroutableTopics(
                fullRequest, routingTable, usesTopicIds);
        Map<String, FetchRequestData> subRequests = fetchDecomposer.decompose(
                fullRequest, routingTable, apiVersion, context::topicName);

        if (subRequests.isEmpty()) {
            var clientResponse = fetchSessionManager.computeClientResponse(errorResponse);
            return CompletableFuture.completedFuture(syntheticResult(context, clientResponse));
        }

        return ensureLeadersCached(subRequests, context).thenCompose(v -> {
            Map<Integer, FetchRequestData> byLeader = groupFetchByLeader(subRequests, fullRequest);

            Map<String, FetchRequestData> byLeaderStr = new HashMap<>();
            for (var entry : byLeader.entrySet()) {
                byLeaderStr.put(String.valueOf(entry.getKey()), entry.getValue());
            }

            fetchSessionManager.wrapForBackends(byLeaderStr);

            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("leaderCount", byLeader.size())
                    .log("Fetch dispatching to partition leaders");

            Map<Integer, CompletionStage<ApiMessage>> futures = new HashMap<>();
            for (var entry : byLeader.entrySet()) {
                futures.put(entry.getKey(),
                        context.sendRequestToNode(entry.getKey(), header, entry.getValue()));
            }

            FetchResponseData capturedErrors = errorResponse;
            return collectAll(futures).thenApply(responses -> {
                Map<String, FetchResponseData> bodies = new HashMap<>();
                for (var entry : responses.entrySet()) {
                    bodies.put(String.valueOf(entry.getKey()),
                            (FetchResponseData) entry.getValue());
                }
                fetchSessionManager.processServerResponses(bodies);
                FetchResponseData merged = fetchDecomposer.recompose(bodies, fullRequest, apiVersion);
                for (var tr : capturedErrors.responses()) {
                    merged.responses().add(tr.duplicate());
                }
                var clientResponse = fetchSessionManager.computeClientResponse(merged);
                return syntheticResult(context, clientResponse);
            });
        });
    }

    private CompletionStage<RouterResponse> handleListOffsets(
                                                              short apiVersion,
                                                              RequestHeaderData header,
                                                              ListOffsetsRequestData request,
                                                              RouterContext context) {
        ListOffsetsResponseData errorResponse = ListOffsetsDecomposer.errorResponseForUnroutableTopics(
                request, routingTable);
        Map<String, ListOffsetsRequestData> subRequests = listOffsetsDecomposer.decompose(
                request, routingTable, apiVersion, context::topicName);

        if (subRequests.isEmpty()) {
            return CompletableFuture.completedFuture(syntheticResult(context, errorResponse));
        }

        return ensureLeadersCached(subRequests, context).thenCompose(v -> {
            Map<Integer, ListOffsetsRequestData> byLeader = groupListOffsetsByLeader(
                    subRequests, request);

            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("leaderCount", byLeader.size())
                    .log("ListOffsets dispatching to partition leaders");

            Map<Integer, CompletionStage<ApiMessage>> futures = new HashMap<>();
            for (var entry : byLeader.entrySet()) {
                futures.put(entry.getKey(),
                        context.sendRequestToNode(entry.getKey(), header, entry.getValue()));
            }

            ListOffsetsResponseData capturedErrors = errorResponse;
            return collectAll(futures).thenApply(responses -> {
                Map<String, ListOffsetsResponseData> bodies = new HashMap<>();
                for (var entry : responses.entrySet()) {
                    bodies.put(String.valueOf(entry.getKey()),
                            (ListOffsetsResponseData) entry.getValue());
                }
                ListOffsetsResponseData merged = listOffsetsDecomposer.recompose(bodies, request, apiVersion);
                for (var tr : capturedErrors.topics()) {
                    merged.topics().add(tr.duplicate());
                }
                refreshCacheIfStaleLeaders(merged, subRequests, context);
                return syntheticResult(context, merged);
            });
        });
    }

    private CompletionStage<RouterResponse> handleOffsetForLeaderEpoch(
                                                                       RequestHeaderData header,
                                                                       OffsetForLeaderEpochRequestData request,
                                                                       RouterContext context) {
        Map<String, OffsetForLeaderEpochRequestData> subRequests = new HashMap<>();
        var errorResponse = new OffsetForLeaderEpochResponseData();

        for (var topic : request.topics()) {
            String route = routingTable.routeForTopic(topic.topic());
            if (route == null) {
                var topicResult = new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult()
                        .setTopic(topic.topic());
                for (var partition : topic.partitions()) {
                    topicResult.partitions().add(
                            new OffsetForLeaderEpochResponseData.EpochEndOffset()
                                    .setPartition(partition.partition())
                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
                }
                errorResponse.topics().add(topicResult);
            }
            else {
                var sub = subRequests.computeIfAbsent(route, k -> new OffsetForLeaderEpochRequestData().setReplicaId(request.replicaId()));
                var subTopic = new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic()
                        .setTopic(topic.topic());
                for (var p : topic.partitions()) {
                    subTopic.partitions().add(p.duplicate());
                }
                sub.topics().add(subTopic);
            }
        }

        if (subRequests.isEmpty()) {
            return CompletableFuture.completedFuture(syntheticResult(context, errorResponse));
        }

        return ensureLeadersCached(subRequests, context).thenCompose(v -> {
            Map<Integer, OffsetForLeaderEpochRequestData> byLeader = new HashMap<>();
            for (var routeEntry : subRequests.entrySet()) {
                for (var topic : routeEntry.getValue().topics()) {
                    for (var partition : topic.partitions()) {
                        Integer leader = leaderForPartition(topic.topic(), partition.partition());
                        if (leader == null) {
                            leader = -1;
                        }
                        var leaderReq = byLeader.computeIfAbsent(leader, k -> new OffsetForLeaderEpochRequestData().setReplicaId(request.replicaId()));
                        var leaderTopic = findOrCreateOffsetForLeaderTopic(leaderReq, topic.topic());
                        leaderTopic.partitions().add(partition.duplicate());
                    }
                }
            }

            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("leaderCount", byLeader.size())
                    .log("OffsetForLeaderEpoch dispatching to partition leaders");

            Map<Integer, CompletionStage<ApiMessage>> futures = new HashMap<>();
            for (var entry : byLeader.entrySet()) {
                futures.put(entry.getKey(),
                        context.sendRequestToNode(entry.getKey(), header, entry.getValue()));
            }

            OffsetForLeaderEpochResponseData capturedErrors = errorResponse;
            return collectAll(futures).thenApply(responses -> {
                var merged = new OffsetForLeaderEpochResponseData();
                for (var entry : responses.entrySet()) {
                    var body = (OffsetForLeaderEpochResponseData) entry.getValue();
                    for (var topicResult : body.topics()) {
                        merged.topics().add(topicResult.duplicate());
                    }
                }
                for (var tr : capturedErrors.topics()) {
                    merged.topics().add(tr.duplicate());
                }
                return syntheticResult(context, merged);
            });
        });
    }

    private static OffsetForLeaderEpochRequestData.OffsetForLeaderTopic findOrCreateOffsetForLeaderTopic(
                                                                                                         OffsetForLeaderEpochRequestData data,
                                                                                                         String topicName) {
        for (var t : data.topics()) {
            if (t.topic().equals(topicName)) {
                return t;
            }
        }
        var t = new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic().setTopic(topicName);
        data.topics().add(t);
        return t;
    }

    private CompletionStage<RouterResponse> handleOffsetCommit(
                                                               short apiVersion,
                                                               RequestHeaderData header,
                                                               OffsetCommitRequestData request,
                                                               RouterContext context) {
        if (!subjectRoutes.isEmpty()) {
            return handleGroupRoutedOffsetCommit(header, request, context);
        }

        OffsetCommitResponseData errorResponse = OffsetCommitDecomposer.errorResponseForUnroutableTopics(
                request, routingTable, apiVersion);
        Map<String, OffsetCommitRequestData> subRequests = offsetCommitDecomposer.decompose(
                request, routingTable, apiVersion, context::topicName);

        if (subRequests.isEmpty()) {
            return CompletableFuture.completedFuture(syntheticResult(context, errorResponse));
        }

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("routeCount", subRequests.size())
                .log("OffsetCommit dispatching to group coordinators");

        Map<String, CompletionStage<ApiMessage>> futures = new HashMap<>();
        for (var entry : subRequests.entrySet()) {
            futures.put(entry.getKey(),
                    sendToGroupCoordinator(entry.getKey(), request.groupId(), header, entry.getValue(), context));
        }

        OffsetCommitResponseData capturedErrors = errorResponse;
        return collectAll(futures).thenApply(responses -> {
            Map<String, OffsetCommitResponseData> bodies = new HashMap<>();
            for (var entry : responses.entrySet()) {
                bodies.put(entry.getKey(), (OffsetCommitResponseData) entry.getValue());
            }
            OffsetCommitResponseData merged = offsetCommitDecomposer.recompose(bodies, request, apiVersion);
            for (var tr : capturedErrors.topics()) {
                merged.topics().add(tr.duplicate());
            }
            return syntheticResult(context, merged);
        });
    }

    private CompletionStage<RouterResponse> handleGroupRoutedOffsetCommit(
                                                                          RequestHeaderData header,
                                                                          OffsetCommitRequestData request,
                                                                          RouterContext context) {
        String expectedRoute = defaultRoute;

        var errorResponse = new OffsetCommitResponseData();
        var routableTopics = new ArrayList<OffsetCommitRequestData.OffsetCommitRequestTopic>();

        for (var topic : request.topics()) {
            String route = routingTable.routeForTopic(topic.name());
            if (route == null || !route.equals(expectedRoute)) {
                var topicResponse = new OffsetCommitResponseTopic().setName(topic.name());
                for (var partition : topic.partitions()) {
                    topicResponse.partitions().add(
                            new OffsetCommitResponsePartition()
                                    .setPartitionIndex(partition.partitionIndex())
                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
                }
                errorResponse.topics().add(topicResponse);
            }
            else {
                routableTopics.add(topic);
            }
        }

        if (routableTopics.isEmpty()) {
            return CompletableFuture.completedFuture(syntheticResult(context, errorResponse));
        }

        var routeRequest = new OffsetCommitRequestData()
                .setGroupId(request.groupId())
                .setGenerationIdOrMemberEpoch(request.generationIdOrMemberEpoch())
                .setMemberId(request.memberId())
                .setGroupInstanceId(request.groupInstanceId())
                .setRetentionTimeMs(request.retentionTimeMs());
        routableTopics.forEach(t -> routeRequest.topics().add(t.duplicate()));

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("route", expectedRoute)
                .log("OffsetCommit routed to consumer group coordinator");

        return sendToGroupCoordinator(expectedRoute, request.groupId(), header, routeRequest, context)
                .thenApply(response -> {
                    if (errorResponse.topics().isEmpty()) {
                        return context.respondWith(response).build();
                    }
                    else {
                        var body = (OffsetCommitResponseData) response;
                        for (var tr : errorResponse.topics()) {
                            body.topics().add(tr.duplicate());
                        }
                        return syntheticResult(context, body);
                    }
                });
    }

    private CompletionStage<RouterResponse> handleCreateTopics(
                                                               short apiVersion,
                                                               RequestHeaderData header,
                                                               CreateTopicsRequestData request,
                                                               RouterContext context) {
        CreateTopicsResponseData errorResponse = CreateTopicsDecomposer.errorResponseForUnroutableTopics(
                request, routingTable);
        CreateTopicsResponseData assignmentErrors = CreateTopicsDecomposer.errorResponseForTopicsWithAssignments(
                request, routingTable);
        for (var tr : List.copyOf(assignmentErrors.topics())) {
            errorResponse.topics().add(tr.duplicate());
            rejectedAssignmentsCounter.increment();
            LOGGER.atWarn()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("topicName", tr.name())
                    .addKeyValue("apiKey", ApiKeys.CREATE_TOPICS)
                    .log("Rejecting CreateTopics with explicit replica assignments");
        }
        Map<String, CreateTopicsRequestData> subRequests = createTopicsDecomposer.decompose(
                request, routingTable, apiVersion, context::topicName);

        if (subRequests.isEmpty()) {
            return CompletableFuture.completedFuture(syntheticResult(context, errorResponse));
        }

        if (subRequests.size() == 1 && errorResponse.topics().isEmpty()) {
            var entry = subRequests.entrySet().iterator().next();
            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("route", entry.getKey())
                    .log("CreateTopics routed to single cluster");
            return context.sendRequestToNode(context.anyNodeId(entry.getKey()), header, entry.getValue())
                    .thenApply(response -> {
                        return context.respondWith(response).build();
                    });
        }

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("routeCount", subRequests.size())
                .log("CreateTopics fanning out across clusters");

        Map<String, CompletionStage<ApiMessage>> futures = new HashMap<>();
        for (var entry : subRequests.entrySet()) {
            futures.put(entry.getKey(),
                    context.sendRequestToNode(context.anyNodeId(entry.getKey()), header, entry.getValue()));
        }

        CreateTopicsResponseData capturedErrors = errorResponse;
        return collectAll(futures).thenApply(responses -> {
            Map<String, CreateTopicsResponseData> bodies = new HashMap<>();
            for (var entry : responses.entrySet()) {
                bodies.put(entry.getKey(), (CreateTopicsResponseData) entry.getValue());
            }
            CreateTopicsResponseData merged = createTopicsDecomposer.recompose(bodies, request, apiVersion);
            for (var tr : capturedErrors.topics()) {
                merged.topics().add(tr.duplicate());
            }
            return syntheticResult(context, merged);
        });
    }

    private CompletionStage<RouterResponse> handleDeleteTopics(
                                                               short apiVersion,
                                                               RequestHeaderData header,
                                                               DeleteTopicsRequestData request,
                                                               RouterContext context) {
        DeleteTopicsResponseData errorResponse = DeleteTopicsDecomposer.errorResponseForUnroutableTopics(
                request, routingTable, apiVersion);
        Map<String, DeleteTopicsRequestData> subRequests = deleteTopicsDecomposer.decompose(
                request, routingTable, apiVersion, context::topicName);

        if (subRequests.isEmpty()) {
            return CompletableFuture.completedFuture(syntheticResult(context, errorResponse));
        }

        if (subRequests.size() == 1 && errorResponse.responses().isEmpty()) {
            var entry = subRequests.entrySet().iterator().next();
            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("route", entry.getKey())
                    .log("DeleteTopics routed to single cluster");
            return context.sendRequestToNode(context.anyNodeId(entry.getKey()), header, entry.getValue())
                    .thenApply(response -> {
                        return context.respondWith(response).build();
                    });
        }

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("routeCount", subRequests.size())
                .log("DeleteTopics fanning out across clusters");

        Map<String, CompletionStage<ApiMessage>> futures = new HashMap<>();
        for (var entry : subRequests.entrySet()) {
            futures.put(entry.getKey(),
                    context.sendRequestToNode(context.anyNodeId(entry.getKey()), header, entry.getValue()));
        }

        DeleteTopicsResponseData capturedErrors = errorResponse;
        return collectAll(futures).thenApply(responses -> {
            Map<String, DeleteTopicsResponseData> bodies = new HashMap<>();
            for (var entry : responses.entrySet()) {
                bodies.put(entry.getKey(), (DeleteTopicsResponseData) entry.getValue());
            }
            DeleteTopicsResponseData merged = deleteTopicsDecomposer.recompose(bodies, request, apiVersion);
            for (var tr : capturedErrors.responses()) {
                merged.responses().add(tr.duplicate());
            }
            return syntheticResult(context, merged);
        });
    }

    private CompletionStage<RouterResponse> handleCreatePartitions(
                                                                   short apiVersion,
                                                                   RequestHeaderData header,
                                                                   CreatePartitionsRequestData request,
                                                                   RouterContext context) {
        CreatePartitionsResponseData errorResponse = CreatePartitionsDecomposer.errorResponseForUnroutableTopics(
                request, routingTable);
        CreatePartitionsResponseData assignmentErrors = CreatePartitionsDecomposer.errorResponseForTopicsWithAssignments(
                request, routingTable);
        for (var tr : assignmentErrors.results()) {
            errorResponse.results().add(tr.duplicate());
            rejectedAssignmentsCounter.increment();
            LOGGER.atWarn()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("topicName", tr.name())
                    .addKeyValue("apiKey", ApiKeys.CREATE_PARTITIONS)
                    .log("Rejecting CreatePartitions with explicit partition assignments");
        }
        Map<String, CreatePartitionsRequestData> subRequests = createPartitionsDecomposer.decompose(
                request, routingTable, apiVersion, context::topicName);

        if (subRequests.isEmpty()) {
            return CompletableFuture.completedFuture(syntheticResult(context, errorResponse));
        }

        if (subRequests.size() == 1 && errorResponse.results().isEmpty()) {
            var entry = subRequests.entrySet().iterator().next();
            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("route", entry.getKey())
                    .log("CreatePartitions routed to single cluster");
            return context.sendRequestToNode(context.anyNodeId(entry.getKey()), header, entry.getValue())
                    .thenApply(response -> {
                        return context.respondWith(response).build();
                    });
        }

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("routeCount", subRequests.size())
                .log("CreatePartitions fanning out across clusters");

        Map<String, CompletionStage<ApiMessage>> futures = new HashMap<>();
        for (var entry : subRequests.entrySet()) {
            futures.put(entry.getKey(),
                    context.sendRequestToNode(context.anyNodeId(entry.getKey()), header, entry.getValue()));
        }

        CreatePartitionsResponseData capturedErrors = errorResponse;
        return collectAll(futures).thenApply(responses -> {
            Map<String, CreatePartitionsResponseData> bodies = new HashMap<>();
            for (var entry : responses.entrySet()) {
                bodies.put(entry.getKey(), (CreatePartitionsResponseData) entry.getValue());
            }
            CreatePartitionsResponseData merged = createPartitionsDecomposer.recompose(bodies, request, apiVersion);
            for (var tr : capturedErrors.results()) {
                merged.results().add(tr.duplicate());
            }
            return syntheticResult(context, merged);
        });
    }

    private CompletionStage<RouterResponse> handleDeleteRecords(
                                                                short apiVersion,
                                                                RequestHeaderData header,
                                                                DeleteRecordsRequestData request,
                                                                RouterContext context) {
        DeleteRecordsResponseData errorResponse = DeleteRecordsDecomposer.errorResponseForUnroutableTopics(
                request, routingTable);
        Map<String, DeleteRecordsRequestData> subRequests = deleteRecordsDecomposer.decompose(
                request, routingTable, apiVersion, context::topicName);

        if (subRequests.isEmpty()) {
            return CompletableFuture.completedFuture(syntheticResult(context, errorResponse));
        }

        return ensureLeadersCached(subRequests, context).thenCompose(v -> {
            Map<Integer, DeleteRecordsRequestData> byLeader = groupDeleteRecordsByLeader(
                    subRequests, request);

            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("leaderCount", byLeader.size())
                    .log("DeleteRecords dispatching to partition leaders");

            Map<Integer, CompletionStage<ApiMessage>> futures = new HashMap<>();
            for (var entry : byLeader.entrySet()) {
                futures.put(entry.getKey(),
                        context.sendRequestToNode(entry.getKey(), header, entry.getValue()));
            }

            DeleteRecordsResponseData capturedErrors = errorResponse;
            return collectAll(futures).thenApply(responses -> {
                Map<String, DeleteRecordsResponseData> bodies = new HashMap<>();
                for (var entry : responses.entrySet()) {
                    bodies.put(String.valueOf(entry.getKey()),
                            (DeleteRecordsResponseData) entry.getValue());
                }
                DeleteRecordsResponseData merged = deleteRecordsDecomposer.recompose(bodies, request, apiVersion);
                for (var tr : capturedErrors.topics()) {
                    merged.topics().add(tr.duplicate());
                }
                refreshCacheIfStaleLeadersDeleteRecords(merged, context);
                return syntheticResult(context, merged);
            });
        });
    }

    private CompletionStage<RouterResponse> handleAddPartitionsToTxn(
                                                                     RequestHeaderData header,
                                                                     AddPartitionsToTxnRequestData request,
                                                                     RouterContext context) {
        String expectedRoute = defaultRoute;
        var topics = request.v3AndBelowTopics();
        var errorTopics = new ArrayList<AddPartitionsToTxnTopicResult>();
        boolean hasRoutableTopic = false;

        for (var topic : topics) {
            String route = routingTable.routeForTopic(topic.name());
            if (route == null) {
                var topicResult = new AddPartitionsToTxnTopicResult().setName(topic.name());
                for (int partition : topic.partitions()) {
                    topicResult.resultsByPartition().add(
                            new AddPartitionsToTxnPartitionResult()
                                    .setPartitionIndex(partition)
                                    .setPartitionErrorCode(
                                            Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
                }
                errorTopics.add(topicResult);
            }
            else if (!route.equals(expectedRoute)) {
                LOGGER.atWarn()
                        .addKeyValue("sessionId", context.sessionId())
                        .addKeyValue("topicName", topic.name())
                        .addKeyValue("topicRoute", route)
                        .addKeyValue("expectedRoute", expectedRoute)
                        .log("ADD_PARTITIONS_TO_TXN topic on wrong route for user");
                return CompletableFuture.completedFuture(syntheticResult(context,
                        allPartitionsError(request, Errors.INVALID_TXN_STATE)));
            }
            else {
                hasRoutableTopic = true;
            }
        }

        if (!hasRoutableTopic) {
            var response = new AddPartitionsToTxnResponseData();
            response.resultsByTopicV3AndBelow().addAll(errorTopics);
            return CompletableFuture.completedFuture(syntheticResult(context, response));
        }

        activeTransactionRoute = expectedRoute;

        long clientPid = request.v3AndBelowProducerId();
        if (!expectedRoute.equals(defaultRoute)) {
            Map<String, ProducerIdEpoch> mapping = producerIdManager.get(clientPid);
            if (mapping == null) {
                LOGGER.atDebug()
                        .addKeyValue("sessionId", context.sessionId())
                        .addKeyValue("clientProducerId", clientPid)
                        .log("Producer ID mapping not found for ADD_PARTITIONS_TO_TXN");
                return CompletableFuture.completedFuture(syntheticResult(context,
                        allPartitionsError(request, Errors.UNKNOWN_PRODUCER_ID)));
            }
            ProducerIdEpoch routeIds = mapping.get(expectedRoute);
            if (routeIds == null) {
                return CompletableFuture.completedFuture(syntheticResult(context,
                        allPartitionsError(request, Errors.UNKNOWN_PRODUCER_ID)));
            }
            request.setV3AndBelowProducerId(routeIds.producerId());
            request.setV3AndBelowProducerEpoch(routeIds.producerEpoch());
        }

        Integer coordinatorNodeId = transactionCoordinators.get(expectedRoute);
        if (coordinatorNodeId == null) {
            LOGGER.atWarn()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("route", expectedRoute)
                    .log("No cached coordinator for route during ADD_PARTITIONS_TO_TXN");
            return CompletableFuture.completedFuture(syntheticResult(context,
                    allPartitionsError(request, Errors.COORDINATOR_NOT_AVAILABLE)));
        }

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("route", expectedRoute)
                .addKeyValue("transactionalId", request.v3AndBelowTransactionalId())
                .log("ADD_PARTITIONS_TO_TXN routed to transaction coordinator");

        List<AddPartitionsToTxnTopicResult> capturedErrors = errorTopics;
        return context.sendRequestToNode(coordinatorNodeId, header, request)
                .thenApply(response -> {
                    if (!capturedErrors.isEmpty()) {
                        var body = (AddPartitionsToTxnResponseData) response;
                        body.resultsByTopicV3AndBelow().addAll(capturedErrors);
                        return syntheticResult(context, body);
                    }
                    else {
                        return context.respondWith(response).build();
                    }
                }).exceptionally(ex -> {
                    LOGGER.atWarn()
                            .addKeyValue("sessionId", context.sessionId())
                            .addKeyValue("route", expectedRoute)
                            .setCause(LOGGER.isDebugEnabled() ? ex : null)
                            .addKeyValue("error", ex.getMessage())
                            .log(LOGGER.isDebugEnabled()
                                    ? "ADD_PARTITIONS_TO_TXN forwarding failed"
                                    : "ADD_PARTITIONS_TO_TXN forwarding failed, "
                                            + "increase log level to DEBUG for stacktrace");
                    return syntheticResult(context,
                            allPartitionsError(request, Errors.COORDINATOR_NOT_AVAILABLE));
                });
    }

    private static AddPartitionsToTxnResponseData allPartitionsError(
                                                                     AddPartitionsToTxnRequestData request,
                                                                     Errors error) {
        var response = new AddPartitionsToTxnResponseData();
        for (var topic : request.v3AndBelowTopics()) {
            var topicResult = new AddPartitionsToTxnTopicResult().setName(topic.name());
            for (int partition : topic.partitions()) {
                topicResult.resultsByPartition().add(
                        new AddPartitionsToTxnPartitionResult()
                                .setPartitionIndex(partition)
                                .setPartitionErrorCode(error.code()));
            }
            response.resultsByTopicV3AndBelow().add(topicResult);
        }
        return response;
    }

    private CompletionStage<RouterResponse> handleAddOffsetsToTxn(
                                                                  RequestHeaderData header,
                                                                  AddOffsetsToTxnRequestData request,
                                                                  RouterContext context) {
        String route = defaultRoute;

        Integer coordinatorNodeId = transactionCoordinators.get(route);
        if (coordinatorNodeId == null) {
            LOGGER.atWarn()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("route", route)
                    .log("No cached coordinator for route during ADD_OFFSETS_TO_TXN");
            return CompletableFuture.completedFuture(syntheticResult(context,
                    new AddOffsetsToTxnResponseData()
                            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())));
        }

        if (!route.equals(defaultRoute)) {
            Map<String, ProducerIdEpoch> mapping = producerIdManager.get(request.producerId());
            if (mapping != null) {
                ProducerIdEpoch routeIds = mapping.get(route);
                if (routeIds != null) {
                    request.setProducerId(routeIds.producerId());
                    request.setProducerEpoch(routeIds.producerEpoch());
                }
            }
        }

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("route", route)
                .addKeyValue("coordinatorNodeId", coordinatorNodeId)
                .addKeyValue("groupId", request.groupId())
                .log("ADD_OFFSETS_TO_TXN routed to transaction coordinator");

        return context.sendRequestToNode(coordinatorNodeId, header, request)
                .<RouterResponse> thenApply(r -> context.respondWith(r).build())
                .exceptionally(ex -> {
                    LOGGER.atWarn()
                            .addKeyValue("sessionId", context.sessionId())
                            .addKeyValue("route", route)
                            .setCause(LOGGER.isDebugEnabled() ? ex : null)
                            .addKeyValue("error", ex.getMessage())
                            .log(LOGGER.isDebugEnabled()
                                    ? "ADD_OFFSETS_TO_TXN forwarding failed"
                                    : "ADD_OFFSETS_TO_TXN forwarding failed, "
                                            + "increase log level to DEBUG for stacktrace");
                    return syntheticResult(context,
                            new AddOffsetsToTxnResponseData()
                                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()));
                });
    }

    private CompletionStage<RouterResponse> handleTxnOffsetCommit(
                                                                  RequestHeaderData header,
                                                                  TxnOffsetCommitRequestData request,
                                                                  RouterContext context) {
        String route = defaultRoute;

        if (!route.equals(defaultRoute)) {
            Map<String, ProducerIdEpoch> mapping = producerIdManager.get(request.producerId());
            if (mapping != null) {
                ProducerIdEpoch routeIds = mapping.get(route);
                if (routeIds != null) {
                    request.setProducerId(routeIds.producerId());
                    request.setProducerEpoch(routeIds.producerEpoch());
                }
            }
        }

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("route", route)
                .addKeyValue("groupId", request.groupId())
                .log("TXN_OFFSET_COMMIT forwarded to group coordinator");

        return sendToGroupCoordinator(route, request.groupId(), header, request, context)
                .<RouterResponse> thenApply(r -> context.respondWith(r).build())
                .exceptionally(ex -> {
                    LOGGER.atWarn()
                            .addKeyValue("sessionId", context.sessionId())
                            .addKeyValue("route", route)
                            .setCause(LOGGER.isDebugEnabled() ? ex : null)
                            .addKeyValue("error", ex.getMessage())
                            .log(LOGGER.isDebugEnabled()
                                    ? "TXN_OFFSET_COMMIT forwarding failed"
                                    : "TXN_OFFSET_COMMIT forwarding failed, "
                                            + "increase log level to DEBUG for stacktrace");
                    var errorResp = new TxnOffsetCommitResponseData();
                    for (var topic : request.topics()) {
                        var topicResp = new TxnOffsetCommitResponseTopic().setName(topic.name());
                        for (var partition : topic.partitions()) {
                            topicResp.partitions().add(
                                    new TxnOffsetCommitResponsePartition()
                                            .setPartitionIndex(partition.partitionIndex())
                                            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()));
                        }
                        errorResp.topics().add(topicResp);
                    }
                    return syntheticResult(context, errorResp);
                });
    }

    private CompletionStage<RouterResponse> handleEndTxn(
                                                         RequestHeaderData header,
                                                         EndTxnRequestData request,
                                                         RouterContext context) {
        String route = defaultRoute;

        Integer coordinatorNodeId = transactionCoordinators.get(route);
        if (coordinatorNodeId == null) {
            return context.sendRequestToNode(context.anyNodeId(route), header, request)
                    .thenApply(response -> {
                        activeTransactionRoute = null;
                        return context.respondWith(response).build();
                    });
        }

        long clientPid = request.producerId();
        short clientEpoch = request.producerEpoch();
        short preRewriteRouteEpoch = -1;

        if (!route.equals(defaultRoute)) {
            Map<String, ProducerIdEpoch> mapping = producerIdManager.get(clientPid);
            if (mapping != null) {
                ProducerIdEpoch routeIds = mapping.get(route);
                if (routeIds != null) {
                    preRewriteRouteEpoch = routeIds.producerEpoch();
                    request.setProducerId(routeIds.producerId());
                    request.setProducerEpoch(routeIds.producerEpoch());
                }
            }
        }

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("route", route)
                .addKeyValue("coordinatorNodeId", coordinatorNodeId)
                .addKeyValue("transactionalId", request.transactionalId())
                .addKeyValue("committed", request.committed())
                .log("END_TXN routed to transaction coordinator");

        short capturedPreRewriteEpoch = preRewriteRouteEpoch;
        return context.sendRequestToNode(coordinatorNodeId, header, request)
                .<RouterResponse> thenApply(response -> {
                    var endTxnResp = (EndTxnResponseData) response;

                    if (endTxnResp.producerId() != -1
                            && !route.equals(defaultRoute)) {
                        producerIdManager.updateRouteEpoch(clientPid, route,
                                new ProducerIdEpoch(
                                        endTxnResp.producerId(),
                                        endTxnResp.producerEpoch()));

                        short newClientEpoch = capturedPreRewriteEpoch >= 0
                                ? (short) (clientEpoch
                                        + (endTxnResp.producerEpoch()
                                                - capturedPreRewriteEpoch))
                                : clientEpoch;
                        endTxnResp.setProducerId(clientPid);
                        endTxnResp.setProducerEpoch(newClientEpoch);

                        LOGGER.atDebug()
                                .addKeyValue("sessionId", context.sessionId())
                                .addKeyValue("route", route)
                                .addKeyValue("clientEpoch", newClientEpoch)
                                .addKeyValue("routeEpoch",
                                        endTxnResp.producerEpoch())
                                .log("END_TXN epoch bump rewritten");
                    }

                    activeTransactionRoute = null;
                    return context.respondWith(response).build();
                }).exceptionally(ex -> {
                    LOGGER.atWarn()
                            .addKeyValue("sessionId", context.sessionId())
                            .addKeyValue("route", route)
                            .setCause(LOGGER.isDebugEnabled() ? ex : null)
                            .addKeyValue("error", ex.getMessage())
                            .log(LOGGER.isDebugEnabled()
                                    ? "END_TXN forwarding failed"
                                    : "END_TXN forwarding failed, "
                                            + "increase log level to DEBUG for stacktrace");
                    activeTransactionRoute = null;
                    return syntheticResult(context,
                            new EndTxnResponseData()
                                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()));
                });
    }

    @Nullable
    private String subjectRouteFor(Subject subject) {
        if (subjectRoutes.isEmpty()) {
            return null;
        }
        return subject.uniquePrincipalOfType(User.class)
                .map(user -> subjectRoutes.get(user.name()))
                .orElse(null);
    }

    private CompletionStage<RouterResponse> forwardToRoute(String route,
                                                           RequestHeaderData header,
                                                           ApiMessage request,
                                                           RouterContext context) {
        return context.sendRequestToNode(context.anyNodeId(route), header, request)
                .thenApply(response -> {
                    return context.respondWith(response).build();
                });
    }

    private CompletionStage<RouterResponse> handleFindCoordinator(
                                                                  RequestHeaderData header,
                                                                  ApiMessage request,
                                                                  RouterContext context) {
        var findCoordReq = (FindCoordinatorRequestData) request;
        String route;
        if (findCoordReq.keyType() == 1) {
            route = defaultRoute;
        }
        else if (findCoordReq.keyType() == 0) {
            route = defaultRoute;
        }
        else {
            route = defaultRoute;
        }

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("route", route)
                .addKeyValue("keyType", findCoordReq.keyType())
                .log("FIND_COORDINATOR forwarded");

        return context.sendRequestToNode(context.anyNodeId(route), header, request)
                .thenApply(response -> {
                    return context.respondWith(response).build();
                });
    }

    private CompletionStage<RouterResponse> handleConsumerGroupHeartbeat(
                                                                         RequestHeaderData header,
                                                                         ConsumerGroupHeartbeatRequestData request,
                                                                         RouterContext context) {
        String route = defaultRoute;
        Integer cachedCoordinator = consumerGroupCoordinators.get(route);

        if (cachedCoordinator != null) {
            return forwardToConsumerGroupCoordinator(
                    cachedCoordinator, route, header, request, context);
        }

        return discoverCoordinator(route, (byte) 0, request.groupId(), context)
                .thenCompose(coordinatorNodeId -> {
                    consumerGroupCoordinators.put(route, coordinatorNodeId);
                    return forwardToConsumerGroupCoordinator(
                            coordinatorNodeId, route, header, request, context);
                }).exceptionally(ex -> {
                    LOGGER.atWarn()
                            .addKeyValue("sessionId", context.sessionId())
                            .addKeyValue("route", route)
                            .addKeyValue("groupId", request.groupId())
                            .setCause(LOGGER.isDebugEnabled() ? ex : null)
                            .addKeyValue("error", ex.getMessage())
                            .log(LOGGER.isDebugEnabled()
                                    ? "CONSUMER_GROUP_HEARTBEAT coordinator discovery failed"
                                    : "CONSUMER_GROUP_HEARTBEAT coordinator discovery failed, "
                                            + "increase log level to DEBUG for stacktrace");
                    return syntheticResult(context,
                            new ConsumerGroupHeartbeatResponseData()
                                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()));
                });
    }

    private CompletionStage<RouterResponse> forwardToConsumerGroupCoordinator(
                                                                              int coordinatorNodeId,
                                                                              String route,
                                                                              RequestHeaderData header,
                                                                              ApiMessage request,
                                                                              RouterContext context) {
        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("route", route)
                .addKeyValue("coordinatorNodeId", coordinatorNodeId)
                .log("Consumer group request forwarded to coordinator");

        return context.sendRequestToNode(coordinatorNodeId, header, request)
                .thenApply(r -> context.respondWith(r).build());
    }

    private CompletionStage<RouterResponse> handleConsumerGroupDescribe(
                                                                        RequestHeaderData header,
                                                                        ConsumerGroupDescribeRequestData request,
                                                                        RouterContext context) {
        String route = defaultRoute;

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("route", route)
                .addKeyValue("groupCount", request.groupIds().size())
                .log("CONSUMER_GROUP_DESCRIBE forwarded to coordinator");

        String groupId = request.groupIds().isEmpty() ? "" : request.groupIds().get(0);
        return sendToGroupCoordinator(route, groupId, header, request, context)
                .thenApply(response -> {
                    return context.respondWith(response).build();
                });
    }

    private CompletionStage<RouterResponse> handleOffsetFetch(
                                                              short apiVersion,
                                                              RequestHeaderData header,
                                                              OffsetFetchRequestData request,
                                                              RouterContext context) {
        String cgRoute = defaultRoute;
        if (!subjectRoutes.isEmpty()) {
            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("route", cgRoute)
                    .log("OffsetFetch routed to consumer group coordinator");
            return sendToGroupCoordinator(cgRoute, request.groupId(), header, request, context)
                    .thenApply(response -> {
                        return context.respondWith(response).build();
                    });
        }

        OffsetFetchResponseData errorResponse = OffsetFetchDecomposer.errorResponseForUnroutableTopics(
                request, routingTable, apiVersion);
        Map<String, OffsetFetchRequestData> subRequests = offsetFetchDecomposer.decompose(
                request, routingTable, apiVersion, context::topicName);

        if (subRequests.isEmpty()) {
            return CompletableFuture.completedFuture(syntheticResult(context, errorResponse));
        }

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("routeCount", subRequests.size())
                .log("OffsetFetch dispatching to group coordinators");

        Map<String, CompletionStage<ApiMessage>> futures = new HashMap<>();
        for (var entry : subRequests.entrySet()) {
            futures.put(entry.getKey(),
                    sendToGroupCoordinator(entry.getKey(), request.groupId(), header, entry.getValue(), context));
        }

        OffsetFetchResponseData capturedErrors = errorResponse;
        return collectAll(futures).thenApply(responses -> {
            Map<String, OffsetFetchResponseData> bodies = new HashMap<>();
            for (var entry : responses.entrySet()) {
                bodies.put(entry.getKey(), (OffsetFetchResponseData) entry.getValue());
            }
            OffsetFetchResponseData merged = offsetFetchDecomposer.recompose(
                    bodies, request, apiVersion);
            mergeOffsetFetchErrors(merged, capturedErrors, apiVersion);
            return syntheticResult(context, merged);
        });
    }

    private static boolean hasOffsetFetchErrors(OffsetFetchResponseData errorResponse,
                                                short apiVersion) {
        if (apiVersion <= 7) {
            return !errorResponse.topics().isEmpty();
        }
        else {
            return !errorResponse.groups().isEmpty();
        }
    }

    private static void mergeOffsetFetchErrors(OffsetFetchResponseData merged,
                                               OffsetFetchResponseData errors,
                                               short apiVersion) {
        if (apiVersion <= 7) {
            for (var tr : errors.topics()) {
                merged.topics().add(tr.duplicate());
            }
        }
        else {
            for (var gr : errors.groups()) {
                merged.groups().add(gr.duplicate());
            }
        }
    }

    private CompletionStage<RouterResponse> handleDescribeCluster(
                                                                  RequestHeaderData header,
                                                                  ApiMessage request,
                                                                  RouterContext context) {
        Set<String> allRoutes = routingTable.allRoutes();

        if (allRoutes.size() == 1) {
            return context.sendRequestToNode(context.anyNodeId(defaultRoute), header, request)
                    .thenApply(response -> {
                        return context.respondWith(response).build();
                    });
        }

        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("routeCount", allRoutes.size())
                .log("DESCRIBE_CLUSTER fanning out across clusters");

        Map<String, CompletionStage<ApiMessage>> futures = new HashMap<>();
        for (String route : allRoutes) {
            futures.put(route, context.sendRequestToNode(context.anyNodeId(route), header, request));
        }

        return collectAll(futures).thenApply(responses -> {
            DescribeClusterResponseData base = null;
            int maxThrottle = 0;

            for (var entry : responses.entrySet()) {
                var resp = (DescribeClusterResponseData) entry.getValue();
                maxThrottle = Math.max(maxThrottle, resp.throttleTimeMs());
                if (entry.getKey().equals(defaultRoute)) {
                    base = resp;
                }
            }

            if (base == null) {
                base = (DescribeClusterResponseData) responses.values().iterator().next();
            }

            var merged = new DescribeClusterResponseData()
                    .setErrorCode(base.errorCode())
                    .setErrorMessage(base.errorMessage())
                    .setClusterId(base.clusterId())
                    .setControllerId(base.controllerId())
                    .setClusterAuthorizedOperations(base.clusterAuthorizedOperations())
                    .setEndpointType(base.endpointType())
                    .setThrottleTimeMs(maxThrottle);

            for (var resp : responses.values()) {
                var body = (DescribeClusterResponseData) resp;
                for (var broker : body.brokers()) {
                    merged.brokers().add(broker.duplicate());
                }
            }

            LOGGER.atDebug()
                    .addKeyValue("sessionId", context.sessionId())
                    .addKeyValue("brokerCount", merged.brokers().size())
                    .addKeyValue("clusterId", merged.clusterId())
                    .log("Merged DESCRIBE_CLUSTER response");

            return syntheticResult(context, merged);
        });
    }

    private static RouterResponse syntheticResult(
                                                  RouterContext context,
                                                  ApiMessage body) {
        var responseHeader = new ResponseHeaderData()
                .setCorrelationId(0);
        return context.respondWith(responseHeader, body).build();
    }

    /**
     * Sends a request to the group coordinator for the given group on the specified route.
     * Uses the cached coordinator if available; otherwise discovers it first.
     */
    private CompletionStage<ApiMessage> sendToGroupCoordinator(
                                                               String route,
                                                               String groupId,
                                                               RequestHeaderData header,
                                                               ApiMessage request,
                                                               RouterContext context) {
        Integer cached = consumerGroupCoordinators.get(route);
        if (cached != null) {
            return context.sendRequestToNode(cached, header, request);
        }
        return discoverCoordinator(route, (byte) 0, groupId, context)
                .thenCompose(coordinatorNodeId -> {
                    consumerGroupCoordinators.put(route, coordinatorNodeId);
                    return context.sendRequestToNode(coordinatorNodeId, header, request);
                });
    }

    /**
     * Sends a request to the transaction coordinator for the given route.
     * Uses the cached coordinator if available; otherwise discovers it first.
     */
    private CompletionStage<ApiMessage> sendToTxnCoordinator(
                                                             String route,
                                                             String transactionalId,
                                                             RequestHeaderData header,
                                                             ApiMessage request,
                                                             RouterContext context) {
        Integer cached = transactionCoordinators.get(route);
        if (cached != null) {
            return context.sendRequestToNode(cached, header, request);
        }
        return discoverCoordinator(route, (byte) 1, transactionalId, context)
                .thenCompose(coordinatorNodeId -> {
                    transactionCoordinators.put(route, coordinatorNodeId);
                    return context.sendRequestToNode(coordinatorNodeId, header, request);
                });
    }

    // package-private for testing
    void updateLeaderCache(MetadataResponseData response) {
        for (var topic : response.topics()) {
            if (topic.errorCode() != Errors.NONE.code()) {
                continue;
            }
            var partMap = partitionLeaders.computeIfAbsent(topic.name(), k -> new HashMap<>());
            for (var partition : topic.partitions()) {
                if (partition.errorCode() == Errors.NONE.code() && partition.leaderId() >= 0) {
                    partMap.put(partition.partitionIndex(), partition.leaderId());
                }
            }
        }
    }

    @Nullable
    private Integer leaderForPartition(String topicName, int partitionIndex) {
        var partMap = partitionLeaders.get(topicName);
        return partMap != null ? partMap.get(partitionIndex) : null;
    }

    private Map<Integer, String> mapLeadersToRoutes(Map<String, ? extends ApiMessage> subRequestsByRoute) {
        Map<Integer, String> leaderToRoute = new HashMap<>();
        for (var entry : subRequestsByRoute.entrySet()) {
            String route = entry.getKey();
            mapLeadersForMessage(entry.getValue(), route, leaderToRoute);
        }
        return leaderToRoute;
    }

    private void mapLeadersForMessage(ApiMessage subRequest, String route, Map<Integer, String> leaderToRoute) {
        if (subRequest instanceof ProduceRequestData pr) {
            for (var topic : pr.topicData()) {
                for (var partition : topic.partitionData()) {
                    Integer leader = leaderForPartition(topic.name(), partition.index());
                    if (leader != null) {
                        leaderToRoute.putIfAbsent(leader, route);
                    }
                }
            }
        }
        else if (subRequest instanceof FetchRequestData fr) {
            for (var topic : fr.topics()) {
                for (var partition : topic.partitions()) {
                    Integer leader = leaderForPartition(topic.topic(), partition.partition());
                    if (leader != null) {
                        leaderToRoute.putIfAbsent(leader, route);
                    }
                }
            }
        }
        else if (subRequest instanceof ListOffsetsRequestData lor) {
            for (var topic : lor.topics()) {
                for (var partition : topic.partitions()) {
                    Integer leader = leaderForPartition(topic.name(), partition.partitionIndex());
                    if (leader != null) {
                        leaderToRoute.putIfAbsent(leader, route);
                    }
                }
            }
        }
        else if (subRequest instanceof DeleteRecordsRequestData drr) {
            for (var topic : drr.topics()) {
                for (var partition : topic.partitions()) {
                    Integer leader = leaderForPartition(topic.name(), partition.partitionIndex());
                    if (leader != null) {
                        leaderToRoute.putIfAbsent(leader, route);
                    }
                }
            }
        }
    }

    /**
     * Sends a METADATA request to the given route for the specified topics
     * and updates the leader cache from the response.
     */
    private CompletionStage<Void> fetchMetadataForTopics(String route,
                                                         List<String> topicNames,
                                                         RouterContext context) {
        var mdHeader = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.METADATA.id)
                .setRequestApiVersion(INTERNAL_METADATA_API_VERSION);
        var mdReq = new MetadataRequestData();
        for (var name : topicNames) {
            mdReq.topics().add(new MetadataRequestData.MetadataRequestTopic().setName(name));
        }
        return context.sendRequestToNode(context.anyNodeId(route), mdHeader, mdReq).thenAccept(response -> {
            updateLeaderCache((MetadataResponseData) response);
        });
    }

    /**
     * Fires a background METADATA request to refresh the leader cache.
     * The response is not awaited — it updates the cache asynchronously
     * on the same event loop thread when the future completes.
     */
    private void refreshLeaderCacheInBackground(String route,
                                                List<String> topicNames,
                                                RouterContext context) {
        LOGGER.atDebug()
                .addKeyValue("sessionId", context.sessionId())
                .addKeyValue("route", route)
                .addKeyValue("topicCount", topicNames.size())
                .log("Refreshing leader cache after stale-leader error");
        fetchMetadataForTopics(route, topicNames, context);
    }

    /**
     * Ensures that leader info is cached for every topic-partition in the
     * decomposed sub-requests. If any leaders are missing, sends METADATA
     * to the relevant route(s) and waits for the response before returning.
     */
    private CompletionStage<Void> ensureLeadersCached(
                                                      Map<String, ? extends ApiMessage> subRequestsByRoute,
                                                      RouterContext context) {
        Map<String, List<String>> uncachedByRoute = new HashMap<>();
        for (var entry : subRequestsByRoute.entrySet()) {
            String route = entry.getKey();
            collectUncachedTopics(entry.getValue(), route, uncachedByRoute);
        }
        if (uncachedByRoute.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletionStage<Void>> fetches = new ArrayList<>();
        for (var entry : uncachedByRoute.entrySet()) {
            fetches.add(fetchMetadataForTopics(entry.getKey(), entry.getValue(), context));
        }
        CompletableFuture<Void> combined = CompletableFuture.completedFuture(null);
        for (var fetch : fetches) {
            combined = combined.thenCombine(fetch, (a, b) -> null);
        }
        return combined;
    }

    private void collectUncachedTopics(ApiMessage subRequest,
                                       String route,
                                       Map<String, List<String>> uncachedByRoute) {
        if (subRequest instanceof ListOffsetsRequestData lor) {
            for (var topic : lor.topics()) {
                for (var partition : topic.partitions()) {
                    if (leaderForPartition(topic.name(), partition.partitionIndex()) == null) {
                        uncachedByRoute.computeIfAbsent(route, k -> new ArrayList<>()).add(topic.name());
                        break;
                    }
                }
            }
        }
        else if (subRequest instanceof DeleteRecordsRequestData drr) {
            for (var topic : drr.topics()) {
                for (var partition : topic.partitions()) {
                    if (leaderForPartition(topic.name(), partition.partitionIndex()) == null) {
                        uncachedByRoute.computeIfAbsent(route, k -> new ArrayList<>()).add(topic.name());
                        break;
                    }
                }
            }
        }
        else if (subRequest instanceof ProduceRequestData pr) {
            for (var topic : pr.topicData()) {
                for (var partition : topic.partitionData()) {
                    if (leaderForPartition(topic.name(), partition.index()) == null) {
                        uncachedByRoute.computeIfAbsent(route, k -> new ArrayList<>()).add(topic.name());
                        break;
                    }
                }
            }
        }
        else if (subRequest instanceof FetchRequestData fr) {
            for (var topic : fr.topics()) {
                for (var partition : topic.partitions()) {
                    if (leaderForPartition(topic.topic(), partition.partition()) == null) {
                        uncachedByRoute.computeIfAbsent(route, k -> new ArrayList<>()).add(topic.topic());
                        break;
                    }
                }
            }
        }
        else if (subRequest instanceof OffsetForLeaderEpochRequestData oflr) {
            for (var topic : oflr.topics()) {
                for (var partition : topic.partitions()) {
                    if (leaderForPartition(topic.topic(), partition.partition()) == null) {
                        uncachedByRoute.computeIfAbsent(route, k -> new ArrayList<>()).add(topic.topic());
                        break;
                    }
                }
            }
        }
    }

    /**
     * Groups LIST_OFFSETS partitions by their cached leader node ID.
     * The router table decomposition has already filtered to routable topics.
     */
    private Map<Integer, ListOffsetsRequestData> groupListOffsetsByLeader(
                                                                          Map<String, ListOffsetsRequestData> subRequestsByRoute,
                                                                          ListOffsetsRequestData original) {
        Map<Integer, ListOffsetsRequestData> byLeader = new HashMap<>();
        for (var routeEntry : subRequestsByRoute.entrySet()) {
            for (var topic : routeEntry.getValue().topics()) {
                for (var partition : topic.partitions()) {
                    Integer leader = leaderForPartition(topic.name(), partition.partitionIndex());
                    if (leader == null) {
                        leader = -1;
                    }
                    var leaderReq = byLeader.computeIfAbsent(leader, k -> new ListOffsetsRequestData()
                            .setReplicaId(original.replicaId())
                            .setIsolationLevel(original.isolationLevel()));
                    var leaderTopic = findOrCreateListOffsetsTopic(leaderReq, topic.name());
                    leaderTopic.partitions().add(partition.duplicate());
                }
            }
        }
        return byLeader;
    }

    private static ListOffsetsRequestData.ListOffsetsTopic findOrCreateListOffsetsTopic(
                                                                                        ListOffsetsRequestData data,
                                                                                        String topicName) {
        for (var t : data.topics()) {
            if (t.name().equals(topicName)) {
                return t;
            }
        }
        var t = new ListOffsetsRequestData.ListOffsetsTopic().setName(topicName);
        data.topics().add(t);
        return t;
    }

    /**
     * Checks if any partition in the response has NOT_LEADER_OR_FOLLOWER or
     * NOT_COORDINATOR, and if so, fires background METADATA refreshes for
     * the affected topics. The original response is returned to the client
     * unchanged.
     */
    private Map<Integer, ProduceRequestData> groupProduceByLeader(
                                                                  Map<String, ProduceRequestData> subRequestsByRoute,
                                                                  ProduceRequestData original) {
        Map<Integer, ProduceRequestData> byLeader = new HashMap<>();
        for (var routeEntry : subRequestsByRoute.entrySet()) {
            var routeReq = routeEntry.getValue();
            for (var topic : routeReq.topicData()) {
                for (var partition : topic.partitionData()) {
                    Integer leader = leaderForPartition(topic.name(), partition.index());
                    if (leader == null) {
                        leader = -1;
                    }
                    var leaderReq = byLeader.computeIfAbsent(leader, k -> new ProduceRequestData()
                            .setAcks(original.acks())
                            .setTimeoutMs(original.timeoutMs())
                            .setTransactionalId(routeReq.transactionalId()));
                    var leaderTopic = findOrCreateProduceTopic(leaderReq, topic.name(), topic.topicId());
                    leaderTopic.partitionData().add(partition.duplicate());
                }
            }
        }
        return byLeader;
    }

    private static ProduceRequestData.TopicProduceData findOrCreateProduceTopic(
                                                                                ProduceRequestData data,
                                                                                String topicName,
                                                                                Uuid topicId) {
        for (var t : data.topicData()) {
            if (t.name().equals(topicName)) {
                return t;
            }
        }
        var t = new ProduceRequestData.TopicProduceData()
                .setName(topicName)
                .setTopicId(topicId);
        data.topicData().add(t);
        return t;
    }

    private Map<Integer, FetchRequestData> groupFetchByLeader(
                                                              Map<String, FetchRequestData> subRequestsByRoute,
                                                              FetchRequestData original) {
        Map<Integer, FetchRequestData> byLeader = new HashMap<>();
        for (var routeEntry : subRequestsByRoute.entrySet()) {
            var routeReq = routeEntry.getValue();
            for (var topic : routeReq.topics()) {
                for (var partition : topic.partitions()) {
                    Integer leader = leaderForPartition(topic.topic(), partition.partition());
                    if (leader == null) {
                        leader = -1;
                    }
                    var leaderReq = byLeader.computeIfAbsent(leader, k -> new FetchRequestData()
                            .setMaxBytes(original.maxBytes())
                            .setMaxWaitMs(original.maxWaitMs())
                            .setMinBytes(original.minBytes())
                            .setIsolationLevel(original.isolationLevel())
                            .setReplicaId(original.replicaId()));
                    var leaderTopic = findOrCreateFetchTopic(leaderReq, topic.topic(), topic.topicId());
                    leaderTopic.partitions().add(partition.duplicate());
                }
            }
        }
        return byLeader;
    }

    private static FetchRequestData.FetchTopic findOrCreateFetchTopic(
                                                                      FetchRequestData data,
                                                                      String topicName,
                                                                      Uuid topicId) {
        for (var t : data.topics()) {
            if (t.topic().equals(topicName)) {
                return t;
            }
        }
        var t = new FetchRequestData.FetchTopic()
                .setTopic(topicName)
                .setTopicId(topicId);
        data.topics().add(t);
        return t;
    }

    private Map<Integer, DeleteRecordsRequestData> groupDeleteRecordsByLeader(
                                                                              Map<String, DeleteRecordsRequestData> subRequestsByRoute,
                                                                              DeleteRecordsRequestData original) {
        Map<Integer, DeleteRecordsRequestData> byLeader = new HashMap<>();
        for (var routeEntry : subRequestsByRoute.entrySet()) {
            for (var topic : routeEntry.getValue().topics()) {
                for (var partition : topic.partitions()) {
                    Integer leader = leaderForPartition(topic.name(), partition.partitionIndex());
                    if (leader == null) {
                        leader = -1;
                    }
                    var leaderReq = byLeader.computeIfAbsent(leader, k -> new DeleteRecordsRequestData()
                            .setTimeoutMs(original.timeoutMs()));
                    var leaderTopic = findOrCreateDeleteRecordsTopic(leaderReq, topic.name());
                    leaderTopic.partitions().add(partition.duplicate());
                }
            }
        }
        return byLeader;
    }

    private static DeleteRecordsRequestData.DeleteRecordsTopic findOrCreateDeleteRecordsTopic(
                                                                                              DeleteRecordsRequestData data,
                                                                                              String topicName) {
        for (var t : data.topics()) {
            if (t.name().equals(topicName)) {
                return t;
            }
        }
        var t = new DeleteRecordsRequestData.DeleteRecordsTopic().setName(topicName);
        data.topics().add(t);
        return t;
    }

    private void refreshCacheIfStaleLeadersDeleteRecords(DeleteRecordsResponseData response,
                                                         RouterContext context) {
        Map<String, List<String>> staleByRoute = new HashMap<>();
        for (var topic : response.topics()) {
            for (var partition : topic.partitions()) {
                if (partition.errorCode() == Errors.NOT_LEADER_OR_FOLLOWER.code()) {
                    String route = routingTable.routeForTopic(topic.name());
                    if (route != null) {
                        staleByRoute.computeIfAbsent(route, k -> new ArrayList<>()).add(topic.name());
                    }
                    break;
                }
            }
        }
        for (var entry : staleByRoute.entrySet()) {
            refreshLeaderCacheInBackground(entry.getKey(), entry.getValue(), context);
        }
    }

    private void refreshCacheIfStaleLeaders(ListOffsetsResponseData response,
                                            Map<String, ListOffsetsRequestData> subRequestsByRoute,
                                            RouterContext context) {
        Map<String, List<String>> staleByRoute = new HashMap<>();
        for (var topic : response.topics()) {
            for (var partition : topic.partitions()) {
                if (partition.errorCode() == Errors.NOT_LEADER_OR_FOLLOWER.code()) {
                    String route = routingTable.routeForTopic(topic.name());
                    if (route != null) {
                        staleByRoute.computeIfAbsent(route, k -> new ArrayList<>()).add(topic.name());
                    }
                    break;
                }
            }
        }
        for (var entry : staleByRoute.entrySet()) {
            refreshLeaderCacheInBackground(entry.getKey(), entry.getValue(), context);
        }
    }

    private static <K> CompletionStage<Map<K, ApiMessage>> collectAll(
                                                                      Map<K, CompletionStage<ApiMessage>> futures) {
        Map<K, ApiMessage> results = new HashMap<>();
        CompletableFuture<Map<K, ApiMessage>> combined = CompletableFuture.completedFuture(results);
        for (var entry : futures.entrySet()) {
            combined = combined.thenCombine(entry.getValue(), (map, response) -> {
                map.put(entry.getKey(), response);
                return map;
            });
        }
        return combined;
    }
}
