/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchRequestData.FetchPartition;
import org.apache.kafka.common.message.FetchRequestData.FetchTopic;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.internal.routing.RoutingEvent;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;

import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for FETCH routing through the topic-partition router.
 */
class FetchRoutingIT extends TopicPartitionRoutingBaseIT {

    private static final String PARAM_TOPIC_A = "a.param";
    private static final String PARAM_TOPIC_B = "b.param";

    @BeforeAll
    static void createParameterisedTestTopics() throws Exception {
        createTopicOnCluster(PARAM_TOPIC_A, 1, clusterA);
        createTopicOnCluster(PARAM_TOPIC_B, 1, clusterB);
    }

    @Test
    void shouldFetchFromTopicsOnBothRoutes() throws Exception {
        String topicA = "a.fetch";
        String topicB = "b.fetch";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            try (var producer = tester.producer(Map.of(
                    "enable.idempotence", false,
                    "retries", 0,
                    "batch.size", 0,
                    "linger.ms", 0))) {
                producer.send(new ProducerRecord<>(topicA, "key-a", "val-a"))
                        .get(10, TimeUnit.SECONDS);
                producer.send(new ProducerRecord<>(topicB, "key-b", "val-b"))
                        .get(10, TimeUnit.SECONDS);
            }

            var consumerProps = new java.util.HashMap<String, Object>();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, tester.getBootstrapAddress());
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

            try (var consumer = new KafkaConsumer<String, String>(consumerProps)) {
                consumer.assign(List.of(
                        new TopicPartition(topicA, 0),
                        new TopicPartition(topicB, 0)));

                List<ConsumerRecord<String, String>> all = new ArrayList<>();
                long deadline = System.currentTimeMillis() + 10_000;
                while (all.size() < 2 && System.currentTimeMillis() < deadline) {
                    consumer.poll(Duration.ofMillis(500)).forEach(all::add);
                }

                assertThat(all).extracting(ConsumerRecord::value)
                        .containsExactlyInAnyOrder("val-a", "val-b");
            }
        }

        var fetchesToA = routingCaptor.requestsToRoute("route-a", ApiKeys.FETCH);
        var fetchesToB = routingCaptor.requestsToRoute("route-b", ApiKeys.FETCH);
        assertThat(fetchesToA).as("FETCH should be routed to route-a").isNotEmpty();
        assertThat(fetchesToB).as("FETCH should be routed to route-b").isNotEmpty();

        for (var event : fetchesToA) {
            var body = (FetchRequestData) event.body();
            assertThat(body.topics()).extracting("topic")
                    .as("route-a should only receive a.* topics")
                    .allSatisfy(name -> assertThat((String) name).startsWith("a."));
        }
        for (var event : fetchesToB) {
            var body = (FetchRequestData) event.body();
            assertThat(body.topics()).extracting("topic")
                    .as("route-b should only receive b.* topics")
                    .allSatisfy(name -> assertThat((String) name).startsWith("b."));
        }
    }

    // --- Version sweep ---

    static List<Arguments> fetchVersions() {
        var result = new ArrayList<Arguments>();
        for (short v = 4; v <= 12; v++) {
            result.add(Arguments.of(v));
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource("fetchVersions")
    void fetchAcrossVersions(short apiVersion) throws Exception {
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            try (var producer = tester.producer(Map.of(
                    "enable.idempotence", false,
                    "retries", 0,
                    "batch.size", 0,
                    "linger.ms", 0))) {
                producer.send(new ProducerRecord<>(PARAM_TOPIC_A, "key-a", "val-a"))
                        .get(10, TimeUnit.SECONDS);
                producer.send(new ProducerRecord<>(PARAM_TOPIC_B, "key-b", "val-b"))
                        .get(10, TimeUnit.SECONDS);
            }

            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                var request = buildFetchRequest(PARAM_TOPIC_A, PARAM_TOPIC_B);
                request.setSessionId(0);
                request.setSessionEpoch(apiVersion >= 7 ? 0 : -1);

                var response = client.getSync(
                        new Request(ApiKeys.FETCH, apiVersion, "test-client", request));
                var body = (FetchResponseData) response.payload().message();

                assertThat(body.responses())
                        .as("v%d FETCH response should contain topics", apiVersion)
                        .isNotEmpty();

                assertThat(body.responses()).extracting(FetchResponseData.FetchableTopicResponse::topic)
                        .as("v%d response should contain both topics", apiVersion)
                        .containsExactlyInAnyOrder(PARAM_TOPIC_A, PARAM_TOPIC_B);

                for (var topicResp : body.responses()) {
                    for (var partResp : topicResp.partitions()) {
                        assertThat(partResp.errorCode())
                                .as("v%d partition error for %s", apiVersion, topicResp.topic())
                                .isEqualTo(Errors.NONE.code());
                    }
                }
            }
        }

        var fetchesToA = routingCaptor.requestsToRoute("route-a", ApiKeys.FETCH);
        var fetchesToB = routingCaptor.requestsToRoute("route-b", ApiKeys.FETCH);
        assertThat(fetchesToA).as("v%d: FETCH should route to route-a", apiVersion).isNotEmpty();
        assertThat(fetchesToB).as("v%d: FETCH should route to route-b", apiVersion).isNotEmpty();

        for (var event : fetchesToA) {
            var body = (FetchRequestData) event.body();
            assertThat(body.topics()).extracting("topic")
                    .as("v%d: route-a should only receive a.* topics", apiVersion)
                    .allSatisfy(name -> assertThat((String) name).startsWith("a."));
        }
        for (var event : fetchesToB) {
            var body = (FetchRequestData) event.body();
            assertThat(body.topics()).extracting("topic")
                    .as("v%d: route-b should only receive b.* topics", apiVersion)
                    .allSatisfy(name -> assertThat((String) name).startsWith("b."));
        }
    }

    // --- Fetch session tests ---

    @Test
    void shouldCreateBackendSessionsOnFirstV12Fetch() throws Exception {
        String topicA = "a.sess";
        String topicB = "b.sess";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            produceOneRecord(tester, topicA, "val-a");
            produceOneRecord(tester, topicB, "val-b");

            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                var request = buildFetchRequest(topicA, topicB);
                request.setSessionId(0);
                request.setSessionEpoch(0);

                var response = client.getSync(
                        new Request(ApiKeys.FETCH, (short) 12, "test-client", request));
                var body = (FetchResponseData) response.payload().message();

                assertThat(body.sessionId())
                        .as("proxy should create a client-side fetch session")
                        .isNotEqualTo(0);
                assertThat(body.errorCode())
                        .isEqualTo(Errors.NONE.code());
            }
        }

        var fetchesToA = routingCaptor.requestsToRoute("route-a", ApiKeys.FETCH);
        var fetchesToB = routingCaptor.requestsToRoute("route-b", ApiKeys.FETCH);
        assertThat(fetchesToA).isNotEmpty();
        assertThat(fetchesToB).isNotEmpty();

        for (var event : fetchesToA) {
            var reqBody = (FetchRequestData) event.body();
            assertThat(reqBody.sessionEpoch())
                    .as("first backend request should request session creation (epoch=0)")
                    .isEqualTo(0);
            assertThat(reqBody.sessionId())
                    .as("first backend request should have sessionId=0")
                    .isEqualTo(0);
        }
        for (var event : fetchesToB) {
            var reqBody = (FetchRequestData) event.body();
            assertThat(reqBody.sessionEpoch()).isEqualTo(0);
            assertThat(reqBody.sessionId()).isEqualTo(0);
        }

        var responsesFromA = fetchResponsesFromRoute("route-a");
        var responsesFromB = fetchResponsesFromRoute("route-b");
        assertThat(responsesFromA).isNotEmpty();
        assertThat(responsesFromB).isNotEmpty();

        for (var event : responsesFromA) {
            var respBody = (FetchResponseData) event.body();
            assertThat(respBody.sessionId())
                    .as("backend route-a should accept session (non-zero sessionId)")
                    .isNotEqualTo(0);
        }
        for (var event : responsesFromB) {
            var respBody = (FetchResponseData) event.body();
            assertThat(respBody.sessionId())
                    .as("backend route-b should accept session (non-zero sessionId)")
                    .isNotEqualTo(0);
        }
    }

    @Test
    void shouldSendIncrementalRequestsOnSubsequentV12Fetch() throws Exception {
        String topicA = "a.incr";
        String topicB = "b.incr";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            produceOneRecord(tester, topicA, "val-a");
            produceOneRecord(tester, topicB, "val-b");

            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                // First fetch: create session
                var firstRequest = buildFetchRequest(topicA, topicB);
                firstRequest.setSessionId(0);
                firstRequest.setSessionEpoch(0);

                var firstResponse = client.getSync(
                        new Request(ApiKeys.FETCH, (short) 12, "test-client", firstRequest));
                var firstBody = (FetchResponseData) firstResponse.payload().message();
                int proxySessionId = firstBody.sessionId();
                assertThat(proxySessionId).as("proxy should create session").isNotEqualTo(0);

                int fetchCountA = routingCaptor.requestsToRoute("route-a", ApiKeys.FETCH).size();
                int fetchCountB = routingCaptor.requestsToRoute("route-b", ApiKeys.FETCH).size();

                // Second fetch: incremental (no partition changes)
                var secondRequest = new FetchRequestData()
                        .setMaxWaitMs(5000)
                        .setMinBytes(1)
                        .setMaxBytes(1024 * 1024)
                        .setSessionId(proxySessionId)
                        .setSessionEpoch(1);

                var secondResponse = client.getSync(
                        new Request(ApiKeys.FETCH, (short) 12, "test-client", secondRequest));
                var secondBody = (FetchResponseData) secondResponse.payload().message();
                assertThat(secondBody.sessionId())
                        .as("session should persist across fetches")
                        .isEqualTo(proxySessionId);
                assertThat(secondBody.errorCode()).isEqualTo(Errors.NONE.code());

                // Backend requests from the second fetch should be incremental
                var allFetchesToA = routingCaptor.requestsToRoute("route-a", ApiKeys.FETCH);
                var secondBatchToA = allFetchesToA.subList(fetchCountA, allFetchesToA.size());
                assertThat(secondBatchToA).as("second fetch should fan out to route-a").isNotEmpty();

                for (var event : secondBatchToA) {
                    var reqBody = (FetchRequestData) event.body();
                    assertThat(reqBody.sessionId())
                            .as("second backend request should use established session")
                            .isNotEqualTo(0);
                    assertThat(reqBody.sessionEpoch())
                            .as("second backend request epoch should be positive")
                            .isGreaterThan(0);
                }

                var allFetchesToB = routingCaptor.requestsToRoute("route-b", ApiKeys.FETCH);
                var secondBatchToB = allFetchesToB.subList(fetchCountB, allFetchesToB.size());
                assertThat(secondBatchToB).as("second fetch should fan out to route-b").isNotEmpty();

                for (var event : secondBatchToB) {
                    var reqBody = (FetchRequestData) event.body();
                    assertThat(reqBody.sessionId())
                            .as("second backend request should use established session")
                            .isNotEqualTo(0);
                    assertThat(reqBody.sessionEpoch())
                            .as("second backend request epoch should be positive")
                            .isGreaterThan(0);
                }
            }
        }
    }

    @Test
    void shouldNotCreateSessionForPreV7Fetch() throws Exception {
        String topicA = "a.prev7";
        String topicB = "b.prev7";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            produceOneRecord(tester, topicA, "val-a");
            produceOneRecord(tester, topicB, "val-b");

            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                var request = buildFetchRequest(topicA, topicB);
                request.setSessionId(0);
                request.setSessionEpoch(-1);

                var response = client.getSync(
                        new Request(ApiKeys.FETCH, (short) 4, "test-client", request));
                var body = (FetchResponseData) response.payload().message();

                assertThat(body.sessionId())
                        .as("pre-v7 fetch should not create a session")
                        .isEqualTo(0);
            }
        }

        for (var event : routingCaptor.requestsToRoute("route-a", ApiKeys.FETCH)) {
            var reqBody = (FetchRequestData) event.body();
            assertThat(reqBody.sessionId()).as("pre-v7 backend sessionId").isEqualTo(0);
            assertThat(reqBody.sessionEpoch()).as("pre-v7 backend epoch").isEqualTo(-1);
        }
        for (var event : routingCaptor.requestsToRoute("route-b", ApiKeys.FETCH)) {
            var reqBody = (FetchRequestData) event.body();
            assertThat(reqBody.sessionId()).as("pre-v7 backend sessionId").isEqualTo(0);
            assertThat(reqBody.sessionEpoch()).as("pre-v7 backend epoch").isEqualTo(-1);
        }
    }

    @Test
    void shouldReturnErrorForUnknownClientSession() throws Exception {
        String topicA = "a.unkn";
        createTopic(topicA, clusterA);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                var request = new FetchRequestData()
                        .setMaxWaitMs(5000)
                        .setMinBytes(1)
                        .setMaxBytes(1024 * 1024)
                        .setSessionId(99999)
                        .setSessionEpoch(1);

                var response = client.getSync(
                        new Request(ApiKeys.FETCH, (short) 12, "test-client", request));
                var body = (FetchResponseData) response.payload().message();

                assertThat(body.errorCode())
                        .as("unknown session should return FETCH_SESSION_ID_NOT_FOUND")
                        .isEqualTo(Errors.FETCH_SESSION_ID_NOT_FOUND.code());
            }
        }

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.FETCH))
                .as("no backend requests should be sent for an invalid session")
                .isEmpty();
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.FETCH))
                .as("no backend requests should be sent for an invalid session")
                .isEmpty();
    }

    @Test
    void shouldFetchSuccessfullyWhenServersDeclineSession(
                                                          @BrokerConfig(name = "max.incremental.fetch.session.cache.slots", value = "0") KafkaCluster noSessionA,
                                                          @BrokerConfig(name = "max.incremental.fetch.session.cache.slots", value = "0") KafkaCluster noSessionB)
            throws Exception {
        String topicA = "a.nosess";
        String topicB = "b.nosess";
        createTopicOnCluster(topicA, 1, noSessionA);
        createTopicOnCluster(topicB, 1, noSessionB);
        var config = topicRouterConfig(noSessionA, noSessionB);

        try (var tester = kroxyliciousTester(config)) {
            produceOneRecord(tester, topicA, "val-a");
            produceOneRecord(tester, topicB, "val-b");

            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                // First fetch: proxy requests session, servers decline
                var firstRequest = buildFetchRequest(topicA, topicB);
                firstRequest.setSessionId(0);
                firstRequest.setSessionEpoch(0);

                var firstResponse = client.getSync(
                        new Request(ApiKeys.FETCH, (short) 12, "test-client", firstRequest));
                var firstBody = (FetchResponseData) firstResponse.payload().message();

                assertThat(firstBody.sessionId())
                        .as("proxy should still create a client-side session")
                        .isNotEqualTo(0);
                assertThat(firstBody.errorCode()).isEqualTo(Errors.NONE.code());
                assertThat(firstBody.responses())
                        .extracting(FetchResponseData.FetchableTopicResponse::topic)
                        .containsExactlyInAnyOrder(topicA, topicB);

                int proxySessionId = firstBody.sessionId();
                int fetchCountA = routingCaptor.requestsToRoute("route-a", ApiKeys.FETCH).size();

                // Second fetch: advance offsets past existing records via
                // incremental modification, so the server returns no records
                // for unchanged partitions on subsequent fetches.
                var secondRequest = new FetchRequestData()
                        .setMaxWaitMs(5000)
                        .setMinBytes(1)
                        .setMaxBytes(1024 * 1024)
                        .setSessionId(proxySessionId)
                        .setSessionEpoch(1);
                var modA = new FetchTopic().setTopic(topicA);
                modA.partitions().add(new FetchPartition()
                        .setPartition(0).setFetchOffset(1).setPartitionMaxBytes(1024 * 1024));
                secondRequest.topics().add(modA);
                var modB = new FetchTopic().setTopic(topicB);
                modB.partitions().add(new FetchPartition()
                        .setPartition(0).setFetchOffset(1).setPartitionMaxBytes(1024 * 1024));
                secondRequest.topics().add(modB);

                var secondResponse = client.getSync(
                        new Request(ApiKeys.FETCH, (short) 12, "test-client", secondRequest));
                var secondBody = (FetchResponseData) secondResponse.payload().message();
                assertThat(secondBody.errorCode()).isEqualTo(Errors.NONE.code());
                assertThat(secondBody.sessionId()).isEqualTo(proxySessionId);

                // Server should receive full (non-incremental) requests since
                // sessions were declined
                var secondBatchToA = routingCaptor.requestsToRoute("route-a", ApiKeys.FETCH)
                        .subList(fetchCountA,
                                routingCaptor.requestsToRoute("route-a", ApiKeys.FETCH).size());
                assertThat(secondBatchToA).isNotEmpty();
                for (var event : secondBatchToA) {
                    var reqBody = (FetchRequestData) event.body();
                    assertThat(reqBody.sessionId())
                            .as("server sessionId should be 0 when sessions are declined")
                            .isEqualTo(0);
                    assertThat(reqBody.sessionEpoch())
                            .as("server epoch should be 0 (session creation retry)")
                            .isEqualTo(0);
                }

                // Produce a new record to topicA only — this changes its HWM
                produceOneRecord(tester, topicA, "val-a2");

                // Third fetch: incremental (no topic changes), still no server
                // sessions. The proxy should return only topicA (new record,
                // changed HWM) and omit topicB (at HWM, no records, unchanged
                // metadata), proving the client session computes incremental
                // responses independently of server session state.
                var thirdRequest = new FetchRequestData()
                        .setMaxWaitMs(5000)
                        .setMinBytes(1)
                        .setMaxBytes(1024 * 1024)
                        .setSessionId(proxySessionId)
                        .setSessionEpoch(2);

                var thirdResponse = client.getSync(
                        new Request(ApiKeys.FETCH, (short) 12, "test-client", thirdRequest));
                var thirdBody = (FetchResponseData) thirdResponse.payload().message();

                assertThat(thirdBody.sessionId()).isEqualTo(proxySessionId);
                assertThat(thirdBody.errorCode()).isEqualTo(Errors.NONE.code());
                assertThat(thirdBody.responses())
                        .extracting(FetchResponseData.FetchableTopicResponse::topic)
                        .as("incremental client response should contain only the changed topic")
                        .containsExactly(topicA);
            }
        }
    }

    @Test
    void shouldCloseClientSessionOnEpochMinusOne() throws Exception {
        String topicA = "a.close";
        String topicB = "b.close";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            produceOneRecord(tester, topicA, "val-a");
            produceOneRecord(tester, topicB, "val-b");

            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                // Create session
                var createRequest = buildFetchRequest(topicA, topicB);
                createRequest.setSessionId(0);
                createRequest.setSessionEpoch(0);

                var createResponse = client.getSync(
                        new Request(ApiKeys.FETCH, (short) 12, "test-client", createRequest));
                int proxySessionId = ((FetchResponseData) createResponse.payload().message()).sessionId();
                assertThat(proxySessionId).isNotEqualTo(0);

                // Close session: sessionId=X, epoch=-1
                var closeRequest = buildFetchRequest(topicA, topicB);
                closeRequest.setSessionId(proxySessionId);
                closeRequest.setSessionEpoch(-1);

                var closeResponse = client.getSync(
                        new Request(ApiKeys.FETCH, (short) 12, "test-client", closeRequest));
                var closeBody = (FetchResponseData) closeResponse.payload().message();

                assertThat(closeBody.sessionId())
                        .as("after session close, sessionId should be 0")
                        .isEqualTo(0);
                assertThat(closeBody.errorCode()).isEqualTo(Errors.NONE.code());

                // Attempting incremental fetch on closed session should fail
                var staleRequest = new FetchRequestData()
                        .setMaxWaitMs(5000)
                        .setMinBytes(1)
                        .setMaxBytes(1024 * 1024)
                        .setSessionId(proxySessionId)
                        .setSessionEpoch(2);

                var staleResponse = client.getSync(
                        new Request(ApiKeys.FETCH, (short) 12, "test-client", staleRequest));
                var staleBody = (FetchResponseData) staleResponse.payload().message();

                assertThat(staleBody.errorCode())
                        .as("closed session should return FETCH_SESSION_ID_NOT_FOUND")
                        .isEqualTo(Errors.FETCH_SESSION_ID_NOT_FOUND.code());
            }
        }
    }

    @Test
    void shouldMaintainServerSessionsWithoutClientSession() throws Exception {
        String topicA = "a.srvsess";
        String topicB = "b.srvsess";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            produceOneRecord(tester, topicA, "val-a");
            produceOneRecord(tester, topicB, "val-b");

            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                // First fetch: no client session (epoch=-1), but proxy
                // should still create server-side sessions
                var firstRequest = buildFetchRequest(topicA, topicB);
                firstRequest.setSessionId(0);
                firstRequest.setSessionEpoch(-1);

                var firstResponse = client.getSync(
                        new Request(ApiKeys.FETCH, (short) 12, "test-client", firstRequest));
                var firstBody = (FetchResponseData) firstResponse.payload().message();

                assertThat(firstBody.sessionId())
                        .as("no client session requested")
                        .isEqualTo(0);
                assertThat(firstBody.responses())
                        .extracting(FetchResponseData.FetchableTopicResponse::topic)
                        .containsExactlyInAnyOrder(topicA, topicB);

                int fetchCountA = routingCaptor.requestsToRoute("route-a", ApiKeys.FETCH).size();
                int fetchCountB = routingCaptor.requestsToRoute("route-b", ApiKeys.FETCH).size();

                // Second fetch: still no client session, but proxy should
                // use established server sessions (incremental)
                var secondRequest = buildFetchRequest(topicA, topicB);
                secondRequest.setSessionId(0);
                secondRequest.setSessionEpoch(-1);

                var secondResponse = client.getSync(
                        new Request(ApiKeys.FETCH, (short) 12, "test-client", secondRequest));
                var secondBody = (FetchResponseData) secondResponse.payload().message();

                assertThat(secondBody.sessionId()).isEqualTo(0);
                assertThat(secondBody.errorCode()).isEqualTo(Errors.NONE.code());
                assertThat(secondBody.responses())
                        .extracting(FetchResponseData.FetchableTopicResponse::topic)
                        .as("client should still see all topics despite incremental server response")
                        .containsExactlyInAnyOrder(topicA, topicB);

                // Verify second requests used server sessions (incremental)
                var secondBatchToA = routingCaptor.requestsToRoute("route-a", ApiKeys.FETCH)
                        .subList(fetchCountA,
                                routingCaptor.requestsToRoute("route-a", ApiKeys.FETCH).size());
                assertThat(secondBatchToA).isNotEmpty();
                for (var event : secondBatchToA) {
                    var reqBody = (FetchRequestData) event.body();
                    assertThat(reqBody.sessionId())
                            .as("server session should be established")
                            .isNotEqualTo(0);
                    assertThat(reqBody.sessionEpoch())
                            .as("should be incremental")
                            .isGreaterThan(0);
                }

                var secondBatchToB = routingCaptor.requestsToRoute("route-b", ApiKeys.FETCH)
                        .subList(fetchCountB,
                                routingCaptor.requestsToRoute("route-b", ApiKeys.FETCH).size());
                assertThat(secondBatchToB).isNotEmpty();
                for (var event : secondBatchToB) {
                    var reqBody = (FetchRequestData) event.body();
                    assertThat(reqBody.sessionId())
                            .as("server session should be established")
                            .isNotEqualTo(0);
                    assertThat(reqBody.sessionEpoch())
                            .as("should be incremental")
                            .isGreaterThan(0);
                }
            }
        }
    }

    // --- helpers ---

    private static FetchRequestData buildFetchRequest(String... topicNames) {
        var request = new FetchRequestData()
                .setMaxWaitMs(5000)
                .setMinBytes(1)
                .setMaxBytes(1024 * 1024)
                .setReplicaId(-1);
        for (var name : topicNames) {
            var topic = new FetchTopic().setTopic(name);
            topic.partitions().add(new FetchPartition()
                    .setPartition(0)
                    .setFetchOffset(0)
                    .setPartitionMaxBytes(1024 * 1024));
            request.topics().add(topic);
        }
        return request;
    }

    private void produceOneRecord(
                                  io.kroxylicious.testing.integration.tester.KroxyliciousTester tester,
                                  String topic,
                                  String value)
            throws Exception {
        try (var producer = tester.producer(Map.of(
                "enable.idempotence", false,
                "retries", 0,
                "batch.size", 0,
                "linger.ms", 0))) {
            producer.send(new ProducerRecord<>(topic, "key", value))
                    .get(10, TimeUnit.SECONDS);
        }
    }

    private List<RoutingEvent.Response> fetchResponsesFromRoute(String route) {
        return routingCaptor.responseEvents().stream()
                .filter(e -> e.route().equals(route))
                .filter(e -> e.apiKey() == ApiKeys.FETCH)
                .toList();
    }
}
