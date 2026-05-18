/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchRequestData.FetchPartition;
import org.apache.kafka.common.message.FetchRequestData.FetchTopic;
import org.apache.kafka.common.message.FetchRequestData.ForgottenTopic;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages bidirectional KIP-227 fetch sessions for a single client connection.
 * Acts as a session server for the downstream client and as a session client
 * for each upstream backend route.
 */
class FetchSessionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(FetchSessionManager.class);

    static final short MIN_SESSION_VERSION = 7;

    private final FetchSessionCache cache;

    private int clientSessionId;
    private int clientNextEpoch;
    private final Map<TopicPartition, PartitionState> clientPartitions = new LinkedHashMap<>();
    private final Map<TopicPartition, CachedPartitionResponse> lastSentToClient = new HashMap<>();

    private final Map<String, ServerSession> serverSessions = new HashMap<>();
    private short lastApiVersion = -1;

    FetchSessionManager(FetchSessionCache cache) {
        this.cache = cache;
    }

    // --- Client-side session management ---

    sealed interface ClientRequestResult {
        record FullFetch(FetchRequestData request) implements ClientRequestResult {}

        record SessionError(FetchResponseData response) implements ClientRequestResult {}
    }

    ClientRequestResult processClientRequest(FetchRequestData request,
                                             short apiVersion) {
        this.lastApiVersion = apiVersion;

        if (apiVersion < MIN_SESSION_VERSION) {
            return new ClientRequestResult.FullFetch(request);
        }

        int sessionId = request.sessionId();
        int epoch = request.sessionEpoch();

        if (sessionId == 0 && epoch == -1) {
            clearClientSession();
            return new ClientRequestResult.FullFetch(request);
        }

        if (epoch == 0) {
            return createClientSession(request);
        }

        if (epoch == -1) {
            if (sessionId != clientSessionId) {
                LOGGER.atDebug()
                        .addKeyValue("sessionId", sessionId)
                        .log("Client fetch session not found");
                return sessionError(Errors.FETCH_SESSION_ID_NOT_FOUND);
            }
            clearClientSession();
            return new ClientRequestResult.FullFetch(request);
        }

        // Incremental: sessionId != 0, epoch > 0
        if (sessionId != clientSessionId || !cache.isValid(clientSessionId)) {
            LOGGER.atDebug()
                    .addKeyValue("sessionId", sessionId)
                    .log("Client fetch session not found");
            if (sessionId == clientSessionId) {
                clearClientSession();
            }
            return sessionError(Errors.FETCH_SESSION_ID_NOT_FOUND);
        }
        if (epoch != clientNextEpoch) {
            LOGGER.atDebug()
                    .addKeyValue("sessionId", sessionId)
                    .addKeyValue("expectedEpoch", clientNextEpoch)
                    .addKeyValue("actualEpoch", epoch)
                    .log("Invalid client fetch session epoch");
            return sessionError(Errors.INVALID_FETCH_SESSION_EPOCH);
        }

        clientNextEpoch = epoch + 1;
        applyIncrementalChanges(request);
        cache.touch(clientSessionId, clientPartitions.size(), System.currentTimeMillis());
        LOGGER.atTrace()
                .addKeyValue("sessionId", clientSessionId)
                .addKeyValue("epoch", epoch)
                .addKeyValue("partitionCount", clientPartitions.size())
                .log("Processed incremental client fetch");
        return new ClientRequestResult.FullFetch(buildFullRequestFromState(request));
    }

    private ClientRequestResult.FullFetch createClientSession(FetchRequestData request) {
        clearClientSession();
        populateClientPartitions(request);
        int id = cache.maybeCreateSession(clientPartitions.size(), System.currentTimeMillis());
        if (id != 0) {
            clientSessionId = id;
            clientNextEpoch = 1;
            LOGGER.atDebug()
                    .addKeyValue("sessionId", id)
                    .addKeyValue("partitionCount", clientPartitions.size())
                    .log("Created client fetch session");
        }
        else {
            LOGGER.atDebug()
                    .addKeyValue("partitionCount", clientPartitions.size())
                    .log("Client fetch session declined by cache");
        }
        return new ClientRequestResult.FullFetch(buildFullRequestFromState(request));
    }

    private void clearClientSession() {
        if (clientSessionId != 0) {
            LOGGER.atDebug()
                    .addKeyValue("sessionId", clientSessionId)
                    .log("Closed client fetch session");
            cache.release(clientSessionId);
        }
        clientSessionId = 0;
        clientNextEpoch = 0;
        clientPartitions.clear();
        lastSentToClient.clear();
    }

    private void populateClientPartitions(FetchRequestData request) {
        clientPartitions.clear();
        for (var topic : request.topics()) {
            for (var partition : topic.partitions()) {
                clientPartitions.put(
                        new TopicPartition(topic.topic(), partition.partition()),
                        PartitionState.from(partition));
            }
        }
    }

    private void applyIncrementalChanges(FetchRequestData request) {
        for (var topic : request.topics()) {
            for (var partition : topic.partitions()) {
                clientPartitions.put(
                        new TopicPartition(topic.topic(), partition.partition()),
                        PartitionState.from(partition));
            }
        }
        for (var forgotten : request.forgottenTopicsData()) {
            for (int partIdx : forgotten.partitions()) {
                clientPartitions.remove(new TopicPartition(forgotten.topic(), partIdx));
            }
        }
    }

    private FetchRequestData buildFullRequestFromState(FetchRequestData original) {
        var full = new FetchRequestData();
        full.setMaxWaitMs(original.maxWaitMs());
        full.setMinBytes(original.minBytes());
        full.setMaxBytes(original.maxBytes());
        full.setIsolationLevel(original.isolationLevel());
        full.setRackId(original.rackId());
        full.setReplicaId(-1);
        full.setSessionId(0);
        full.setSessionEpoch(-1);

        Map<String, FetchTopic> topicMap = new LinkedHashMap<>();
        for (var entry : clientPartitions.entrySet()) {
            var tp = entry.getKey();
            var state = entry.getValue();
            topicMap.computeIfAbsent(tp.topic(), t -> new FetchTopic().setTopic(t))
                    .partitions().add(state.toFetchPartition(tp.partition()));
        }
        for (var topic : topicMap.values()) {
            full.topics().add(topic);
        }
        return full;
    }

    private static ClientRequestResult.SessionError sessionError(Errors error) {
        var response = new FetchResponseData();
        response.setErrorCode(error.code());
        response.setSessionId(0);
        return new ClientRequestResult.SessionError(response);
    }

    // --- Backend session management ---

    void wrapForBackends(Map<String, FetchRequestData> subRequests) {
        if (lastApiVersion < MIN_SESSION_VERSION) {
            for (var sub : subRequests.values()) {
                sub.setSessionId(0);
                sub.setSessionEpoch(-1);
            }
            return;
        }

        for (var entry : subRequests.entrySet()) {
            wrapForBackend(entry.getKey(), entry.getValue());
        }
    }

    private void wrapForBackend(String route,
                                FetchRequestData subRequest) {
        ServerSession session = serverSessions.computeIfAbsent(route, k -> new ServerSession());

        if (session.sessionId == 0) {
            session.recordPartitions(subRequest);
            subRequest.setSessionId(0);
            subRequest.setSessionEpoch(0);
            return;
        }

        Map<TopicPartition, PartitionState> currentPartitions = extractPartitions(subRequest);

        Map<String, FetchTopic> changedTopics = new LinkedHashMap<>();
        for (var entry : currentPartitions.entrySet()) {
            TopicPartition tp = entry.getKey();
            PartitionState current = entry.getValue();
            PartitionState last = session.lastSentPartitions.get(tp);
            if (last == null || !last.equals(current)) {
                changedTopics.computeIfAbsent(tp.topic(), t -> new FetchTopic().setTopic(t))
                        .partitions().add(current.toFetchPartition(tp.partition()));
            }
        }

        Map<String, List<Integer>> removedByTopic = new LinkedHashMap<>();
        for (var tp : session.lastSentPartitions.keySet()) {
            if (!currentPartitions.containsKey(tp)) {
                removedByTopic.computeIfAbsent(tp.topic(), k -> new ArrayList<>())
                        .add(tp.partition());
            }
        }

        subRequest.topics().clear();
        for (var topic : changedTopics.values()) {
            subRequest.topics().add(topic);
        }
        subRequest.forgottenTopicsData().clear();
        for (var entry : removedByTopic.entrySet()) {
            subRequest.forgottenTopicsData().add(
                    new ForgottenTopic()
                            .setTopic(entry.getKey())
                            .setPartitions(entry.getValue()));
        }
        subRequest.setSessionId(session.sessionId);
        subRequest.setSessionEpoch(session.nextEpoch);

        session.nextEpoch++;
        session.lastSentPartitions.clear();
        session.lastSentPartitions.putAll(currentPartitions);
    }

    void processServerResponses(Map<String, FetchResponseData> responses) {
        for (var entry : responses.entrySet()) {
            String route = entry.getKey();
            FetchResponseData response = entry.getValue();
            ServerSession session = serverSessions.get(route);
            if (session == null) {
                continue;
            }

            if (response.errorCode() == Errors.FETCH_SESSION_ID_NOT_FOUND.code()) {
                session.reset();
                continue;
            }

            if (response.sessionId() != 0 && session.sessionId == 0) {
                session.sessionId = response.sessionId();
                session.nextEpoch = 1;
            }

            if (session.sessionId != 0) {
                reconstructFullResponse(response, session);
            }
        }
    }

    private static void reconstructFullResponse(FetchResponseData response,
                                                ServerSession session) {
        Map<TopicPartition, PartitionData> present = new HashMap<>();
        for (var topicResp : response.responses()) {
            for (var partition : topicResp.partitions()) {
                var tp = new TopicPartition(topicResp.topic(), partition.partitionIndex());
                present.put(tp, partition);
                session.cachedResponses.put(tp, CachedPartitionResponse.from(partition));
            }
        }

        Map<String, FetchableTopicResponse> synthesised = new LinkedHashMap<>();
        for (var cached : session.cachedResponses.entrySet()) {
            if (!present.containsKey(cached.getKey())) {
                var tp = cached.getKey();
                synthesised.computeIfAbsent(
                        tp.topic(),
                        t -> new FetchableTopicResponse().setTopic(t))
                        .partitions().add(cached.getValue().toPartitionData(tp.partition()));
            }
        }

        for (var topicResp : synthesised.values()) {
            var existing = response.responses().stream()
                    .filter(t -> t.topic().equals(topicResp.topic()))
                    .findFirst();
            if (existing.isPresent()) {
                existing.get().partitions().addAll(topicResp.partitions());
            }
            else {
                response.responses().add(topicResp);
            }
        }
    }

    // --- Client response computation ---

    FetchResponseData computeClientResponse(FetchResponseData mergedResponse) {
        if (lastApiVersion < MIN_SESSION_VERSION || clientSessionId == 0) {
            mergedResponse.setSessionId(0);
            mergedResponse.setErrorCode(Errors.NONE.code());
            return mergedResponse;
        }

        if (lastSentToClient.isEmpty()) {
            mergedResponse.setSessionId(clientSessionId);
            mergedResponse.setErrorCode(Errors.NONE.code());
            updateLastSentToClient(mergedResponse);
            return mergedResponse;
        }

        var incremental = new FetchResponseData();
        incremental.setSessionId(clientSessionId);
        incremental.setErrorCode(Errors.NONE.code());
        incremental.setThrottleTimeMs(mergedResponse.throttleTimeMs());

        for (var topicResp : mergedResponse.responses()) {
            FetchableTopicResponse incTopic = null;
            for (var partition : topicResp.partitions()) {
                var tp = new TopicPartition(topicResp.topic(), partition.partitionIndex());
                var lastSent = lastSentToClient.get(tp);

                if (lastSent == null || lastSent.changed(partition)) {
                    if (incTopic == null) {
                        incTopic = new FetchableTopicResponse().setTopic(topicResp.topic());
                    }
                    incTopic.partitions().add(partition);
                    lastSentToClient.put(tp, CachedPartitionResponse.from(partition));
                }
            }
            if (incTopic != null) {
                incremental.responses().add(incTopic);
            }
        }

        return incremental;
    }

    private void updateLastSentToClient(FetchResponseData response) {
        for (var topicResp : response.responses()) {
            for (var partition : topicResp.partitions()) {
                lastSentToClient.put(
                        new TopicPartition(topicResp.topic(), partition.partitionIndex()),
                        CachedPartitionResponse.from(partition));
            }
        }
    }

    // --- Accessors for testing ---

    int clientSessionId() {
        return clientSessionId;
    }

    // --- Helpers ---

    private static Map<TopicPartition, PartitionState> extractPartitions(FetchRequestData request) {
        Map<TopicPartition, PartitionState> result = new LinkedHashMap<>();
        for (var topic : request.topics()) {
            for (var partition : topic.partitions()) {
                result.put(
                        new TopicPartition(topic.topic(), partition.partition()),
                        PartitionState.from(partition));
            }
        }
        return result;
    }

    // --- Inner types ---

    record PartitionState(
                          long fetchOffset,
                          int partitionMaxBytes,
                          int currentLeaderEpoch,
                          long logStartOffset,
                          int lastFetchedEpoch) {

        static PartitionState from(FetchPartition partition) {
            return new PartitionState(
                    partition.fetchOffset(),
                    partition.partitionMaxBytes(),
                    partition.currentLeaderEpoch(),
                    partition.logStartOffset(),
                    partition.lastFetchedEpoch());
        }

        FetchPartition toFetchPartition(int partitionIndex) {
            return new FetchPartition()
                    .setPartition(partitionIndex)
                    .setFetchOffset(fetchOffset)
                    .setPartitionMaxBytes(partitionMaxBytes)
                    .setCurrentLeaderEpoch(currentLeaderEpoch)
                    .setLogStartOffset(logStartOffset)
                    .setLastFetchedEpoch(lastFetchedEpoch);
        }
    }

    record CachedPartitionResponse(
                                   short errorCode,
                                   long highWatermark,
                                   long lastStableOffset,
                                   long logStartOffset) {

        static CachedPartitionResponse from(PartitionData partition) {
            return new CachedPartitionResponse(
                    partition.errorCode(),
                    partition.highWatermark(),
                    partition.lastStableOffset(),
                    partition.logStartOffset());
        }

        PartitionData toPartitionData(int partitionIndex) {
            return new PartitionData()
                    .setPartitionIndex(partitionIndex)
                    .setErrorCode(errorCode)
                    .setHighWatermark(highWatermark)
                    .setLastStableOffset(lastStableOffset)
                    .setLogStartOffset(logStartOffset);
        }

        boolean changed(PartitionData partition) {
            return errorCode != partition.errorCode()
                    || highWatermark != partition.highWatermark()
                    || lastStableOffset != partition.lastStableOffset()
                    || logStartOffset != partition.logStartOffset()
                    || hasRecords(partition);
        }

        private static boolean hasRecords(PartitionData partition) {
            if (partition.records() == null) {
                return false;
            }
            if (partition.records() instanceof MemoryRecords mr) {
                return mr.sizeInBytes() > 0;
            }
            return true;
        }
    }

    static class ServerSession {
        int sessionId;
        int nextEpoch;
        final Map<TopicPartition, PartitionState> lastSentPartitions = new LinkedHashMap<>();
        final Map<TopicPartition, CachedPartitionResponse> cachedResponses = new HashMap<>();

        void recordPartitions(FetchRequestData request) {
            lastSentPartitions.clear();
            for (var topic : request.topics()) {
                for (var partition : topic.partitions()) {
                    lastSentPartitions.put(
                            new TopicPartition(topic.topic(), partition.partition()),
                            PartitionState.from(partition));
                }
            }
        }

        void reset() {
            sessionId = 0;
            nextEpoch = 0;
            lastSentPartitions.clear();
            cachedResponses.clear();
        }
    }
}
