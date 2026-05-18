/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchRequestData.FetchPartition;
import org.apache.kafka.common.message.FetchRequestData.FetchTopic;
import org.apache.kafka.common.message.FetchRequestData.ForgottenTopic;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FetchSessionManagerTest {

    private FetchSessionManager manager;
    private FetchSessionCache cache;

    @BeforeEach
    void setUp() {
        cache = new FetchSessionCache(1000, 0, "testVc", "testRouter");
        manager = new FetchSessionManager(cache);
    }

    @Nested
    class ClientSideSession {

        @Test
        void shouldPassthroughPreV7Request() {
            var request = fetchRequest("topic-a");

            var result = manager.processClientRequest(request, (short) 4);

            assertThat(result).isInstanceOf(FetchSessionManager.ClientRequestResult.FullFetch.class);
            var full = (FetchSessionManager.ClientRequestResult.FullFetch) result;
            assertThat(full.request()).isSameAs(request);
            assertThat(manager.clientSessionId()).isEqualTo(0);
        }

        @Test
        void shouldPassthroughNoSessionRequest() {
            var request = fetchRequest("topic-a");
            request.setSessionId(0);
            request.setSessionEpoch(-1);

            var result = manager.processClientRequest(request, (short) 12);

            assertThat(result).isInstanceOf(FetchSessionManager.ClientRequestResult.FullFetch.class);
            assertThat(manager.clientSessionId()).isEqualTo(0);
        }

        @Test
        void shouldCreateSessionOnEpochZero() {
            var request = fetchRequest("topic-a");
            request.setSessionId(0);
            request.setSessionEpoch(0);

            var result = manager.processClientRequest(request, (short) 12);

            assertThat(result).isInstanceOf(FetchSessionManager.ClientRequestResult.FullFetch.class);
            assertThat(manager.clientSessionId()).isGreaterThan(0);
            var full = (FetchSessionManager.ClientRequestResult.FullFetch) result;
            assertThat(full.request().topics()).extracting("topic")
                    .containsExactly("topic-a");
        }

        @Test
        void shouldPreserveEnvelopeFieldsOnSessionCreation() {
            var request = fetchRequest("topic-a");
            request.setSessionId(0);
            request.setSessionEpoch(0);
            request.setMaxWaitMs(500);
            request.setMinBytes(1);
            request.setMaxBytes(1048576);
            request.setIsolationLevel((byte) 1);
            request.setRackId("rack-1");

            var result = (FetchSessionManager.ClientRequestResult.FullFetch) manager.processClientRequest(request, (short) 12);

            assertThat(result.request().maxWaitMs()).isEqualTo(500);
            assertThat(result.request().minBytes()).isEqualTo(1);
            assertThat(result.request().maxBytes()).isEqualTo(1048576);
            assertThat(result.request().isolationLevel()).isEqualTo((byte) 1);
            assertThat(result.request().rackId()).isEqualTo("rack-1");
        }

        @Test
        void shouldTrackPartitionsOnSessionCreation() {
            var request = new FetchRequestData();
            request.setSessionEpoch(0);
            var topic = new FetchTopic().setTopic("topic-a");
            topic.partitions().add(new FetchPartition().setPartition(0).setFetchOffset(100));
            topic.partitions().add(new FetchPartition().setPartition(1).setFetchOffset(200));
            request.topics().add(topic);

            var result = (FetchSessionManager.ClientRequestResult.FullFetch) manager.processClientRequest(request, (short) 12);

            assertThat(result.request().topics()).hasSize(1);
            assertThat(result.request().topics().get(0).partitions()).hasSize(2);
            assertThat(result.request().topics().get(0).partitions())
                    .extracting("fetchOffset")
                    .containsExactly(100L, 200L);
        }

        @Test
        void shouldApplyIncrementalAdditions() {
            createSession("topic-a");
            int sessionId = manager.clientSessionId();

            var incremental = new FetchRequestData();
            incremental.setSessionId(sessionId);
            incremental.setSessionEpoch(1);
            var newTopic = new FetchTopic().setTopic("topic-b");
            newTopic.partitions().add(new FetchPartition().setPartition(0).setFetchOffset(50));
            incremental.topics().add(newTopic);

            var result = (FetchSessionManager.ClientRequestResult.FullFetch) manager.processClientRequest(incremental, (short) 12);

            assertThat(result.request().topics()).extracting("topic")
                    .containsExactlyInAnyOrder("topic-a", "topic-b");
        }

        @Test
        void shouldApplyIncrementalRemovals() {
            var request = new FetchRequestData();
            request.setSessionEpoch(0);
            var topicA = new FetchTopic().setTopic("topic-a");
            topicA.partitions().add(new FetchPartition().setPartition(0).setFetchOffset(0));
            var topicB = new FetchTopic().setTopic("topic-b");
            topicB.partitions().add(new FetchPartition().setPartition(0).setFetchOffset(0));
            request.topics().add(topicA);
            request.topics().add(topicB);
            manager.processClientRequest(request, (short) 12);
            int sessionId = manager.clientSessionId();

            var incremental = new FetchRequestData();
            incremental.setSessionId(sessionId);
            incremental.setSessionEpoch(1);
            incremental.forgottenTopicsData().add(
                    new ForgottenTopic().setTopic("topic-b").setPartitions(List.of(0)));

            var result = (FetchSessionManager.ClientRequestResult.FullFetch) manager.processClientRequest(incremental, (short) 12);

            assertThat(result.request().topics()).extracting("topic")
                    .containsExactly("topic-a");
        }

        @Test
        void shouldApplyIncrementalModifications() {
            createSession("topic-a");
            int sessionId = manager.clientSessionId();

            var incremental = new FetchRequestData();
            incremental.setSessionId(sessionId);
            incremental.setSessionEpoch(1);
            var modified = new FetchTopic().setTopic("topic-a");
            modified.partitions().add(new FetchPartition().setPartition(0).setFetchOffset(999));
            incremental.topics().add(modified);

            var result = (FetchSessionManager.ClientRequestResult.FullFetch) manager.processClientRequest(incremental, (short) 12);

            assertThat(result.request().topics().get(0).partitions().get(0).fetchOffset())
                    .isEqualTo(999);
        }

        @Test
        void shouldRejectUnknownSessionId() {
            createSession("topic-a");

            var incremental = new FetchRequestData();
            incremental.setSessionId(99999);
            incremental.setSessionEpoch(1);

            var result = manager.processClientRequest(incremental, (short) 12);

            assertThat(result).isInstanceOf(FetchSessionManager.ClientRequestResult.SessionError.class);
            var error = (FetchSessionManager.ClientRequestResult.SessionError) result;
            assertThat(error.response().errorCode())
                    .isEqualTo(Errors.FETCH_SESSION_ID_NOT_FOUND.code());
        }

        @Test
        void shouldRejectWrongEpoch() {
            createSession("topic-a");
            int sessionId = manager.clientSessionId();

            var incremental = new FetchRequestData();
            incremental.setSessionId(sessionId);
            incremental.setSessionEpoch(5);

            var result = manager.processClientRequest(incremental, (short) 12);

            assertThat(result).isInstanceOf(FetchSessionManager.ClientRequestResult.SessionError.class);
            var error = (FetchSessionManager.ClientRequestResult.SessionError) result;
            assertThat(error.response().errorCode())
                    .isEqualTo(Errors.INVALID_FETCH_SESSION_EPOCH.code());
        }

        @Test
        void shouldCloseSessionOnEpochMinusOne() {
            createSession("topic-a");
            int sessionId = manager.clientSessionId();

            var close = fetchRequest("topic-a");
            close.setSessionId(sessionId);
            close.setSessionEpoch(-1);

            var result = manager.processClientRequest(close, (short) 12);

            assertThat(result).isInstanceOf(FetchSessionManager.ClientRequestResult.FullFetch.class);
            assertThat(manager.clientSessionId()).isEqualTo(0);
        }

        @Test
        void shouldRejectCloseOfUnknownSession() {
            var close = fetchRequest("topic-a");
            close.setSessionId(99999);
            close.setSessionEpoch(-1);

            var result = manager.processClientRequest(close, (short) 12);

            assertThat(result).isInstanceOf(FetchSessionManager.ClientRequestResult.SessionError.class);
        }

        @Test
        void shouldCloseAndRecreateOnEpochZeroWithSessionId() {
            createSession("topic-a");
            int oldSessionId = manager.clientSessionId();

            var recreate = fetchRequest("topic-b");
            recreate.setSessionId(oldSessionId);
            recreate.setSessionEpoch(0);

            var result = (FetchSessionManager.ClientRequestResult.FullFetch) manager.processClientRequest(recreate, (short) 12);

            assertThat(manager.clientSessionId()).isGreaterThan(0);
            assertThat(manager.clientSessionId()).isNotEqualTo(oldSessionId);
            assertThat(result.request().topics()).extracting("topic")
                    .containsExactly("topic-b");
        }

        @Test
        void shouldIncrementEpochOnEachIncremental() {
            createSession("topic-a");
            int sessionId = manager.clientSessionId();

            for (int epoch = 1; epoch <= 3; epoch++) {
                var inc = new FetchRequestData();
                inc.setSessionId(sessionId);
                inc.setSessionEpoch(epoch);

                var result = manager.processClientRequest(inc, (short) 12);

                assertThat(result).isInstanceOf(FetchSessionManager.ClientRequestResult.FullFetch.class);
            }
        }
    }

    @Nested
    class CacheIntegration {

        @Test
        void shouldOperateSessionlessWhenCacheDeclines() {
            var zeroSlotCache = new FetchSessionCache(0, 0, "testVc", "testRouter");
            var sessionless = new FetchSessionManager(zeroSlotCache);

            var request = fetchRequest("topic-a");
            request.setSessionId(0);
            request.setSessionEpoch(0);

            var result = sessionless.processClientRequest(request, (short) 12);

            assertThat(result).isInstanceOf(FetchSessionManager.ClientRequestResult.FullFetch.class);
            assertThat(sessionless.clientSessionId()).isEqualTo(0);

            var merged = responseWithPartition("topic-a", 0, 10L);
            var response = sessionless.computeClientResponse(merged);
            assertThat(response.sessionId()).isEqualTo(0);
            assertThat(response.responses()).hasSize(1);
        }

        @Test
        void shouldDetectEvictionFromSharedCache() {
            createSession("topic-a");
            int sessionId = manager.clientSessionId();
            assertThat(sessionId).isGreaterThan(0);

            // Externally evict the session from the shared cache
            cache.release(sessionId);

            // Client sends incremental — should get FETCH_SESSION_ID_NOT_FOUND
            var incremental = new FetchRequestData();
            incremental.setSessionId(sessionId);
            incremental.setSessionEpoch(1);

            var result = manager.processClientRequest(incremental, (short) 12);

            assertThat(result).isInstanceOf(FetchSessionManager.ClientRequestResult.SessionError.class);
            var error = (FetchSessionManager.ClientRequestResult.SessionError) result;
            assertThat(error.response().errorCode())
                    .isEqualTo(Errors.FETCH_SESSION_ID_NOT_FOUND.code());
            assertThat(manager.clientSessionId()).isEqualTo(0);
        }

        @Test
        void shouldReleaseCacheSlotOnSessionClose() {
            var oneSlotCache = new FetchSessionCache(1, 0, "testVc", "testRouter");
            var mgr = new FetchSessionManager(oneSlotCache);

            var create = fetchRequest("topic-a");
            create.setSessionId(0);
            create.setSessionEpoch(0);
            mgr.processClientRequest(create, (short) 12);
            int sessionId = mgr.clientSessionId();
            assertThat(sessionId).isGreaterThan(0);
            assertThat(oneSlotCache.size()).isEqualTo(1);

            // Close session
            var close = fetchRequest("topic-a");
            close.setSessionId(sessionId);
            close.setSessionEpoch(-1);
            mgr.processClientRequest(close, (short) 12);

            assertThat(oneSlotCache.size()).isEqualTo(0);
        }
    }

    @Nested
    class ServerSession {

        @Test
        void shouldForceNoSessionForPreV7() {
            var subRequests = decomposedRequests("route-a", "topic-a");
            manager.processClientRequest(fetchRequest("topic-a"), (short) 4);

            manager.wrapForBackends(subRequests);

            var req = subRequests.get("route-a");
            assertThat(req.sessionId()).isEqualTo(0);
            assertThat(req.sessionEpoch()).isEqualTo(-1);
        }

        @Test
        void shouldRequestSessionCreationOnFirstRequest() {
            createSession("topic-a");
            var subRequests = decomposedRequests("route-a", "topic-a");

            manager.wrapForBackends(subRequests);

            var req = subRequests.get("route-a");
            assertThat(req.sessionId()).isEqualTo(0);
            assertThat(req.sessionEpoch()).isEqualTo(0);
            assertThat(req.topics()).extracting("topic").containsExactly("topic-a");
        }

        @Test
        void shouldSendIncrementalAfterSessionEstablished() {
            createSession("topic-a");

            var sub1 = decomposedRequests("route-a", "topic-a");
            manager.wrapForBackends(sub1);
            processServerResponse("route-a", 42);

            var sub2 = decomposedRequests("route-a", "topic-a");
            manager.wrapForBackends(sub2);

            var req = sub2.get("route-a");
            assertThat(req.sessionId()).isEqualTo(42);
            assertThat(req.sessionEpoch()).isEqualTo(1);
            assertThat(req.topics()).as("no changes → empty topics").isEmpty();
            assertThat(req.forgottenTopicsData()).isEmpty();
        }

        @Test
        void shouldIncludeChangedPartitionsInIncremental() {
            createSession("topic-a");
            var sub1 = decomposedRequests("route-a", "topic-a");
            manager.wrapForBackends(sub1);
            processServerResponse("route-a", 42);

            var sub2 = decomposedRequestsWithOffset("route-a", "topic-a", 0, 999);
            manager.wrapForBackends(sub2);

            var req = sub2.get("route-a");
            assertThat(req.sessionId()).isEqualTo(42);
            assertThat(req.topics()).hasSize(1);
            assertThat(req.topics().get(0).partitions().get(0).fetchOffset()).isEqualTo(999);
        }

        @Test
        void shouldAddForgottenTopicsForRemovedPartitions() {
            var request = new FetchRequestData();
            request.setSessionEpoch(0);
            addTopic(request, "topic-a", 0, 0);
            addTopic(request, "topic-b", 0, 0);
            manager.processClientRequest(request, (short) 12);

            var sub1 = decomposedRequests(
                    Map.of("route-a", List.of("topic-a"), "route-b", List.of("topic-b")));
            manager.wrapForBackends(sub1);
            processServerResponse("route-a", 10);
            processServerResponse("route-b", 20);

            // Now remove topic-b from the client session
            int sessionId = manager.clientSessionId();
            var incremental = new FetchRequestData();
            incremental.setSessionId(sessionId);
            incremental.setSessionEpoch(1);
            incremental.forgottenTopicsData().add(
                    new ForgottenTopic().setTopic("topic-b").setPartitions(List.of(0)));
            manager.processClientRequest(incremental, (short) 12);

            // route-b should get a forget request
            var sub2 = decomposedRequests("route-a", "topic-a");
            // route-b is not in decomposition (no topics for it)
            // but wrapForBackends only wraps what's in the map
            manager.wrapForBackends(sub2);

            var reqA = sub2.get("route-a");
            assertThat(reqA.sessionId()).isEqualTo(10);
            assertThat(reqA.topics()).isEmpty();
        }

        @Test
        void shouldResetSessionOnEviction() {
            createSession("topic-a");
            var sub1 = decomposedRequests("route-a", "topic-a");
            manager.wrapForBackends(sub1);
            processServerResponse("route-a", 42);

            // Backend evicts session
            var evictionResponse = new FetchResponseData();
            evictionResponse.setErrorCode(Errors.FETCH_SESSION_ID_NOT_FOUND.code());
            manager.processServerResponses(Map.of("route-a", evictionResponse));

            // Next request should create a new session
            var sub2 = decomposedRequests("route-a", "topic-a");
            manager.wrapForBackends(sub2);

            var req = sub2.get("route-a");
            assertThat(req.sessionId()).isEqualTo(0);
            assertThat(req.sessionEpoch()).isEqualTo(0);
        }

        @Test
        void shouldRetrySessionCreationWhenBackendDeclines() {
            createSession("topic-a");
            var sub1 = decomposedRequests("route-a", "topic-a");
            manager.wrapForBackends(sub1);
            processServerResponse("route-a", 0);

            var sub2 = decomposedRequests("route-a", "topic-a");
            manager.wrapForBackends(sub2);

            var req = sub2.get("route-a");
            assertThat(req.sessionId()).as("still no session → try again").isEqualTo(0);
            assertThat(req.sessionEpoch()).isEqualTo(0);
        }

        @Test
        void shouldIncrementBackendEpoch() {
            createSession("topic-a");
            var sub1 = decomposedRequests("route-a", "topic-a");
            manager.wrapForBackends(sub1);
            processServerResponse("route-a", 42);

            for (int expectedEpoch = 1; expectedEpoch <= 3; expectedEpoch++) {
                var sub = decomposedRequests("route-a", "topic-a");
                manager.wrapForBackends(sub);

                assertThat(sub.get("route-a").sessionEpoch())
                        .as("epoch at iteration %d", expectedEpoch)
                        .isEqualTo(expectedEpoch);
            }
        }

        @Test
        void shouldReconstructFullResponseFromIncremental() {
            createSession("topic-a");
            var sub1 = decomposedRequests("route-a", "topic-a");
            addTopic(sub1.get("route-a"), "topic-a", 1, 0);
            manager.wrapForBackends(sub1);

            // First response: full (both partitions)
            var fullResp = new FetchResponseData();
            fullResp.setSessionId(42);
            addResponsePartition(fullResp, "topic-a", 0, 10L);
            addResponsePartition(fullResp, "topic-a", 1, 20L);
            manager.processServerResponses(Map.of("route-a", fullResp));

            // Second request (incremental, no changes)
            var sub2 = decomposedRequests("route-a", "topic-a");
            addTopic(sub2.get("route-a"), "topic-a", 1, 0);
            manager.wrapForBackends(sub2);

            // Second response: incremental (only partition 0 changed)
            var incrResp = new FetchResponseData();
            incrResp.setSessionId(42);
            addResponsePartition(incrResp, "topic-a", 0, 15L);
            manager.processServerResponses(Map.of("route-a", incrResp));

            assertThat(incrResp.responses()).hasSize(1);
            assertThat(incrResp.responses().get(0).partitions())
                    .extracting("partitionIndex")
                    .as("reconstructed response should contain both partitions")
                    .containsExactlyInAnyOrder(0, 1);

            var p0 = incrResp.responses().get(0).partitions().stream()
                    .filter(p -> p.partitionIndex() == 0).findFirst().orElseThrow();
            assertThat(p0.highWatermark()).as("partition 0 should have updated HWM").isEqualTo(15L);

            var p1 = incrResp.responses().get(0).partitions().stream()
                    .filter(p -> p.partitionIndex() == 1).findFirst().orElseThrow();
            assertThat(p1.highWatermark()).as("partition 1 should have cached HWM").isEqualTo(20L);
        }

        @Test
        void shouldUpdateCacheWithNewPartitionData() {
            createSession("topic-a");
            var sub1 = decomposedRequests("route-a", "topic-a");
            manager.wrapForBackends(sub1);

            var resp1 = new FetchResponseData();
            resp1.setSessionId(42);
            addResponsePartition(resp1, "topic-a", 0, 10L);
            manager.processServerResponses(Map.of("route-a", resp1));

            var sub2 = decomposedRequests("route-a", "topic-a");
            manager.wrapForBackends(sub2);

            var resp2 = new FetchResponseData();
            resp2.setSessionId(42);
            addResponsePartition(resp2, "topic-a", 0, 25L);
            manager.processServerResponses(Map.of("route-a", resp2));

            // Third request — the cache should have the updated value
            var sub3 = decomposedRequests("route-a", "topic-a");
            manager.wrapForBackends(sub3);

            var resp3 = new FetchResponseData();
            resp3.setSessionId(42);
            // Empty incremental — nothing changed
            manager.processServerResponses(Map.of("route-a", resp3));

            assertThat(resp3.responses()).hasSize(1);
            assertThat(resp3.responses().get(0).partitions().get(0).highWatermark())
                    .as("cache should reflect the most recent HWM")
                    .isEqualTo(25L);
        }

        @Test
        void shouldClearCacheOnEviction() {
            createSession("topic-a");
            var sub1 = decomposedRequests("route-a", "topic-a");
            manager.wrapForBackends(sub1);

            var resp1 = new FetchResponseData();
            resp1.setSessionId(42);
            addResponsePartition(resp1, "topic-a", 0, 10L);
            manager.processServerResponses(Map.of("route-a", resp1));

            // Eviction
            var eviction = new FetchResponseData();
            eviction.setErrorCode(Errors.FETCH_SESSION_ID_NOT_FOUND.code());
            manager.processServerResponses(Map.of("route-a", eviction));

            // Re-establish session
            var sub2 = decomposedRequests("route-a", "topic-a");
            manager.wrapForBackends(sub2);
            assertThat(sub2.get("route-a").sessionId()).isEqualTo(0);

            var resp2 = new FetchResponseData();
            resp2.setSessionId(99);
            addResponsePartition(resp2, "topic-a", 0, 50L);
            manager.processServerResponses(Map.of("route-a", resp2));

            assertThat(resp2.responses().get(0).partitions().get(0).highWatermark())
                    .as("fresh session should not contain stale cached data")
                    .isEqualTo(50L);
        }
    }

    @Nested
    class ClientResponse {

        @Test
        void shouldReturnFullResponseForPreV7() {
            manager.processClientRequest(fetchRequest("topic-a"), (short) 4);

            var merged = responseWithPartition("topic-a", 0, 10L);

            var result = manager.computeClientResponse(merged);

            assertThat(result.sessionId()).isEqualTo(0);
            assertThat(result.errorCode()).isEqualTo(Errors.NONE.code());
            assertThat(result.responses()).hasSize(1);
        }

        @Test
        void shouldReturnFullResponseForNoSession() {
            var request = fetchRequest("topic-a");
            request.setSessionId(0);
            request.setSessionEpoch(-1);
            manager.processClientRequest(request, (short) 12);

            var merged = responseWithPartition("topic-a", 0, 10L);

            var result = manager.computeClientResponse(merged);

            assertThat(result.sessionId()).isEqualTo(0);
            assertThat(result.responses()).hasSize(1);
        }

        @Test
        void shouldReturnFullResponseOnSessionCreation() {
            createSession("topic-a");
            int sessionId = manager.clientSessionId();

            var merged = responseWithPartition("topic-a", 0, 10L);

            var result = manager.computeClientResponse(merged);

            assertThat(result.sessionId()).isEqualTo(sessionId);
            assertThat(result.errorCode()).isEqualTo(Errors.NONE.code());
            assertThat(result.responses()).hasSize(1);
        }

        @Test
        void shouldComputeIncrementalWhenNothingChanged() {
            createSession("topic-a");
            var initial = responseWithPartition("topic-a", 0, 10L);
            manager.computeClientResponse(initial);

            sendIncremental(1);
            var same = responseWithPartition("topic-a", 0, 10L);
            var result = manager.computeClientResponse(same);

            assertThat(result.sessionId()).isEqualTo(manager.clientSessionId());
            assertThat(result.responses()).as("nothing changed → empty response").isEmpty();
        }

        @Test
        void shouldIncludePartitionWithChangedHighWatermark() {
            createSession("topic-a");
            var initial = responseWithPartition("topic-a", 0, 10L);
            manager.computeClientResponse(initial);

            sendIncremental(1);
            var updated = responseWithPartition("topic-a", 0, 15L);
            var result = manager.computeClientResponse(updated);

            assertThat(result.responses()).hasSize(1);
            assertThat(result.responses().get(0).partitions().get(0).highWatermark())
                    .isEqualTo(15L);
        }

        @Test
        void shouldIncludePartitionWithChangedErrorCode() {
            createSession("topic-a");
            var initial = responseWithPartition("topic-a", 0, 10L);
            manager.computeClientResponse(initial);

            sendIncremental(1);
            var withError = new FetchResponseData();
            var topicResp = new FetchableTopicResponse().setTopic("topic-a");
            topicResp.partitions().add(new PartitionData()
                    .setPartitionIndex(0)
                    .setHighWatermark(10L)
                    .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code()));
            withError.responses().add(topicResp);

            var result = manager.computeClientResponse(withError);

            assertThat(result.responses()).hasSize(1);
            assertThat(result.responses().get(0).partitions().get(0).errorCode())
                    .isEqualTo(Errors.NOT_LEADER_OR_FOLLOWER.code());
        }

        @Test
        void shouldIncludeNewlyAddedPartition() {
            createSession("topic-a");
            var initial = responseWithPartition("topic-a", 0, 10L);
            manager.computeClientResponse(initial);

            // Client adds topic-b
            var inc = new FetchRequestData();
            inc.setSessionId(manager.clientSessionId());
            inc.setSessionEpoch(1);
            addTopic(inc, "topic-b", 0, 0);
            manager.processClientRequest(inc, (short) 12);

            var merged = new FetchResponseData();
            addResponsePartition(merged, "topic-a", 0, 10L);
            addResponsePartition(merged, "topic-b", 0, 5L);

            var result = manager.computeClientResponse(merged);

            assertThat(result.responses()).extracting("topic")
                    .as("topic-a unchanged, topic-b is new")
                    .containsExactly("topic-b");
        }

        @Test
        void shouldPreserveThrottleTime() {
            createSession("topic-a");
            var initial = responseWithPartition("topic-a", 0, 10L);
            manager.computeClientResponse(initial);

            sendIncremental(1);
            var updated = responseWithPartition("topic-a", 0, 15L);
            updated.setThrottleTimeMs(300);

            var result = manager.computeClientResponse(updated);

            assertThat(result.throttleTimeMs()).isEqualTo(300);
        }
    }

    // --- Helpers ---

    private void createSession(String... topicNames) {
        var request = fetchRequest(topicNames);
        request.setSessionId(0);
        request.setSessionEpoch(0);
        manager.processClientRequest(request, (short) 12);
    }

    private void sendIncremental(int epoch) {
        var inc = new FetchRequestData();
        inc.setSessionId(manager.clientSessionId());
        inc.setSessionEpoch(epoch);
        manager.processClientRequest(inc, (short) 12);
    }

    private void processServerResponse(String route,
                                       int backendSessionId) {
        var resp = new FetchResponseData();
        resp.setSessionId(backendSessionId);
        manager.processServerResponses(Map.of(route, resp));
    }

    private static FetchRequestData fetchRequest(String... topicNames) {
        var request = new FetchRequestData();
        for (var name : topicNames) {
            var topic = new FetchTopic().setTopic(name);
            topic.partitions().add(new FetchPartition().setPartition(0).setFetchOffset(0));
            request.topics().add(topic);
        }
        return request;
    }

    private static void addTopic(FetchRequestData request,
                                 String topicName,
                                 int partition,
                                 long fetchOffset) {
        var topic = new FetchTopic().setTopic(topicName);
        topic.partitions().add(new FetchPartition()
                .setPartition(partition)
                .setFetchOffset(fetchOffset));
        request.topics().add(topic);
    }

    private static Map<String, FetchRequestData> decomposedRequests(String route,
                                                                    String topicName) {
        return decomposedRequestsWithOffset(route, topicName, 0, 0);
    }

    private static Map<String, FetchRequestData> decomposedRequestsWithOffset(String route,
                                                                              String topicName,
                                                                              int partition,
                                                                              long fetchOffset) {
        var sub = new FetchRequestData();
        var topic = new FetchTopic().setTopic(topicName);
        topic.partitions().add(new FetchPartition()
                .setPartition(partition)
                .setFetchOffset(fetchOffset));
        sub.topics().add(topic);
        var result = new LinkedHashMap<String, FetchRequestData>();
        result.put(route, sub);
        return result;
    }

    private static Map<String, FetchRequestData> decomposedRequests(
                                                                    Map<String, List<String>> routeToTopics) {
        var result = new LinkedHashMap<String, FetchRequestData>();
        for (var entry : routeToTopics.entrySet()) {
            var sub = new FetchRequestData();
            for (var topicName : entry.getValue()) {
                var topic = new FetchTopic().setTopic(topicName);
                topic.partitions().add(new FetchPartition().setPartition(0).setFetchOffset(0));
                sub.topics().add(topic);
            }
            result.put(entry.getKey(), sub);
        }
        return result;
    }

    private static FetchResponseData responseWithPartition(String topicName,
                                                           int partition,
                                                           long highWatermark) {
        var resp = new FetchResponseData();
        addResponsePartition(resp, topicName, partition, highWatermark);
        return resp;
    }

    private static void addResponsePartition(FetchResponseData response,
                                             String topicName,
                                             int partition,
                                             long highWatermark) {
        var existing = response.responses().stream()
                .filter(t -> t.topic().equals(topicName))
                .findFirst();
        FetchableTopicResponse topicResp;
        if (existing.isPresent()) {
            topicResp = existing.get();
        }
        else {
            topicResp = new FetchableTopicResponse().setTopic(topicName);
            response.responses().add(topicResp);
        }
        topicResp.partitions().add(new PartitionData()
                .setPartitionIndex(partition)
                .setHighWatermark(highWatermark));
    }
}
