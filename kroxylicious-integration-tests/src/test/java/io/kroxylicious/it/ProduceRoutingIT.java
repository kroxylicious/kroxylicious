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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.it.testplugins.FaultInjectionFilterFactory;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.tester.KroxyliciousTester;

import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Integration tests for PRODUCE routing through the topic-partition router.
 */
class ProduceRoutingIT extends TopicPartitionRoutingBaseIT {

    private static final String PARAM_TOPIC_A = "a.param";
    private static final String PARAM_TOPIC_B = "b.param";

    @BeforeAll
    static void createParameterisedTestTopics() throws Exception {
        createTopicOnCluster(PARAM_TOPIC_A, 1, clusterA);
        createTopicOnCluster(PARAM_TOPIC_B, 1, clusterB);
    }

    // --- Basic produce routing ---

    @Test
    void shouldPassThroughProduceAndConsumeWithSingleRoute() throws Exception {
        String topic = "a.passthrough";
        createTopic(topic, clusterA);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0))) {
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>(topic, "key-" + i, "val-" + i))
                        .get(10, TimeUnit.SECONDS);
            }
        }

        var records = consumeDirectly(clusterA, topic);
        assertThat(records).hasSize(5);
        assertThat(records).extracting(ConsumerRecord::value)
                .containsExactly("val-0", "val-1", "val-2", "val-3", "val-4");

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .as("all produces should go to route-a")
                .hasSize(5);
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.PRODUCE))
                .as("no produces should go to route-b")
                .isEmpty();
    }

    @Test
    void shouldFanOutProduceToMultipleClusters() throws Exception {
        String topicA = "a.orders";
        String topicB = "b.logs";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0))) {
            producer.send(new ProducerRecord<>(topicA, "key-a1", "val-a1"))
                    .get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicB, "key-b1", "val-b1"))
                    .get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicA, "key-a2", "val-a2"))
                    .get(10, TimeUnit.SECONDS);
        }

        var recordsA = consumeDirectly(clusterA, topicA);
        var recordsB = consumeDirectly(clusterB, topicB);

        assertThat(recordsA).hasSize(2);
        assertThat(recordsA).extracting(ConsumerRecord::value)
                .containsExactly("val-a1", "val-a2");

        assertThat(recordsB).hasSize(1);
        assertThat(recordsB).extracting(ConsumerRecord::value)
                .containsExactly("val-b1");

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .as("a.orders produces to route-a")
                .hasSize(2);
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.PRODUCE))
                .as("b.logs produces to route-b")
                .hasSize(1);
    }

    @Test
    void shouldFanOutAcksZeroProduce() throws Exception {
        String topicA = "a.fire";
        String topicB = "b.forget";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0,
                        "acks", "0"))) {
            producer.send(new ProducerRecord<>(topicA, "key-a", "val-a"));
            producer.send(new ProducerRecord<>(topicB, "key-b", "val-b"));
            producer.flush();
        }

        var recordsA = consumeDirectly(clusterA, topicA);
        var recordsB = consumeDirectly(clusterB, topicB);

        assertThat(recordsA).hasSize(1);
        assertThat(recordsB).hasSize(1);

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .as("fire-and-forget produce to route-a")
                .hasSize(1);
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.PRODUCE))
                .as("fire-and-forget produce to route-b")
                .hasSize(1);
    }

    @Test
    void shouldRouteUnprefixedTopicToDefaultRoute() throws Exception {
        String topic = "unprefixed-topic";
        createTopic(topic, clusterA);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0))) {
            producer.send(new ProducerRecord<>(topic, "key", "val"))
                    .get(10, TimeUnit.SECONDS);
        }

        var records = consumeDirectly(clusterA, topic);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).value()).isEqualTo("val");

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .as("unprefixed topic should route to default route")
                .hasSize(1);
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.PRODUCE))
                .isEmpty();
    }

    // --- Idempotent produce ---

    @Test
    void shouldProduceWithIdempotenceToMultipleRoutes() throws Exception {
        String topicA = "a.idempotent";
        String topicB = "b.idempotent";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);

        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", true,
                        "retries", 3,
                        "batch.size", 0,
                        "linger.ms", 0))) {
            producer.send(new ProducerRecord<>(topicA, "key-a", "val-a"))
                    .get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicB, "key-b", "val-b"))
                    .get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicA, "key-a2", "val-a2"))
                    .get(10, TimeUnit.SECONDS);
        }

        var recordsA = consumeDirectly(clusterA, topicA);
        var recordsB = consumeDirectly(clusterB, topicB);

        assertThat(recordsA).hasSize(2);
        assertThat(recordsA).extracting(ConsumerRecord::value)
                .containsExactly("val-a", "val-a2");

        assertThat(recordsB).hasSize(1);
        assertThat(recordsB).extracting(ConsumerRecord::value)
                .containsExactly("val-b");

        // INIT_PRODUCER_ID fanned out to both routes
        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.INIT_PRODUCER_ID))
                .as("INIT_PRODUCER_ID to route-a")
                .isNotEmpty();
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.INIT_PRODUCER_ID))
                .as("INIT_PRODUCER_ID to route-b")
                .isNotEmpty();

        // Extract the producer ID each cluster allocated from INIT_PRODUCER_ID responses
        long allocatedPidA = initProducerIdFromResponse(routingCaptor, "route-a");
        long allocatedPidB = initProducerIdFromResponse(routingCaptor, "route-b");
        assertThat(allocatedPidA).as("route-a producer ID").isNotEqualTo(RecordBatch.NO_PRODUCER_ID);
        assertThat(allocatedPidB).as("route-b producer ID").isNotEqualTo(RecordBatch.NO_PRODUCER_ID);

        // PRODUCE to each route uses the route-specific producer ID
        var produceToA = routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE);
        var produceToB = routingCaptor.requestsToRoute("route-b", ApiKeys.PRODUCE);
        assertThat(produceToA).hasSize(2);
        assertThat(produceToB).hasSize(1);

        produceToA.forEach(event -> RoutingRequestEventAssert.assertThat(event).hasProducerId(allocatedPidA));
        produceToB.forEach(event -> RoutingRequestEventAssert.assertThat(event).hasProducerId(allocatedPidB));

        // Sequence numbers per route start from 0 and increment
        RoutingRequestEventAssert.assertThat(produceToA.get(0)).hasBaseSequence(0);
        RoutingRequestEventAssert.assertThat(produceToA.get(1)).hasBaseSequence(1);
        RoutingRequestEventAssert.assertThat(produceToB.get(0)).hasBaseSequence(0);
    }

    @Test
    void shouldProduceWithIdempotenceToNonDefaultRoute() throws Exception {
        String topicB = "b.idem-nondefault";
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", true,
                        "retries", 3,
                        "batch.size", 0,
                        "linger.ms", 0))) {
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>(topicB, "key-" + i, "val-" + i))
                        .get(10, TimeUnit.SECONDS);
            }
        }

        var records = consumeDirectly(clusterB, topicB);
        assertThat(records).hasSize(5);
        assertThat(records).extracting(ConsumerRecord::value)
                .containsExactly("val-0", "val-1", "val-2", "val-3", "val-4");

        // All produces to route-b with consistent producer ID
        var produces = routingCaptor.requestsToRoute("route-b", ApiKeys.PRODUCE);
        assertThat(produces).hasSize(5);

        long pid = produces.get(0).firstProducerId().orElseThrow();
        produces.forEach(event -> RoutingRequestEventAssert.assertThat(event).hasProducerId(pid));

        // Sequence numbers increment from 0
        for (int i = 0; i < produces.size(); i++) {
            RoutingRequestEventAssert.assertThat(produces.get(i)).hasBaseSequence(i);
        }
    }

    // --- Fault injection ---

    @Test
    void shouldDisconnectClientWhenFaultInjected() throws Exception {
        String topicA = "a.fault";
        createTopic(topicA, clusterA);

        FaultInjectionFilterFactory.reset();
        var config = topicRouterConfig()
                .addNewFilterDefinition(
                        "fault",
                        FaultInjectionFilterFactory.class.getName(),
                        null)
                .addToDefaultFilters("fault");

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0,
                        "request.timeout.ms", 5000,
                        "delivery.timeout.ms", 10000))) {

            producer.send(new ProducerRecord<>(topicA, "before", "ok"))
                    .get(10, TimeUnit.SECONDS);

            var records = consumeDirectly(clusterA, topicA);
            assertThat(records).hasSize(1);
            assertThat(records.get(0).value()).isEqualTo("ok");

            FaultInjectionFilterFactory.latestHandle().closeOnNextRequest();

            assertThat(catchThrowable(() -> producer.send(new ProducerRecord<>(topicA, "after", "should-fail"))
                    .get(10, TimeUnit.SECONDS)))
                    .as("produce should fail after fault injection")
                    .isInstanceOf(ExecutionException.class);
        }

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .as("at least one produce should have reached route-a before fault")
                .isNotEmpty();
    }

    @Test
    void shouldPreserveIdempotencyAcrossDisconnect() throws Exception {
        String topicA = "a.idem-reconnect";
        String topicB = "b.idem-reconnect";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);

        FaultInjectionFilterFactory.reset();
        var config = topicRouterConfig()
                .addNewFilterDefinition(
                        "fault",
                        FaultInjectionFilterFactory.class.getName(),
                        null)
                .addToDefaultFilters("fault");

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", true,
                        "retries", 5,
                        "batch.size", 0,
                        "linger.ms", 0))) {

            // Phase 1: produce successfully before disconnect
            producer.send(new ProducerRecord<>(topicA, "k1", "before-a1"))
                    .get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicB, "k1", "before-b1"))
                    .get(10, TimeUnit.SECONDS);

            // Phase 2: inject fault — producer disconnects and auto-reconnects
            FaultInjectionFilterFactory.latestHandle().closeOnNextRequest();
            producer.send(new ProducerRecord<>(topicA, "k2", "reconnect-a"))
                    .get(30, TimeUnit.SECONDS);

            // Phase 3: produce more after reconnect
            producer.send(new ProducerRecord<>(topicA, "k3", "after-a1"))
                    .get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicB, "k2", "after-b1"))
                    .get(10, TimeUnit.SECONDS);
        }

        // Black-box: no duplicates on either backend, in order
        var recordsA = consumeDirectly(clusterA, topicA);
        var recordsB = consumeDirectly(clusterB, topicB);
        assertThat(recordsA).extracting(ConsumerRecord::value)
                .containsExactly("before-a1", "reconnect-a", "after-a1");
        assertThat(recordsB).extracting(ConsumerRecord::value)
                .containsExactly("before-b1", "after-b1");

        // White-box: per-route PIDs are consistent across the disconnect
        long pidA = initProducerIdFromResponse(routingCaptor, "route-a");
        long pidB = initProducerIdFromResponse(routingCaptor, "route-b");
        assertThat(pidA).as("route-a producer ID").isNotEqualTo(RecordBatch.NO_PRODUCER_ID);
        assertThat(pidB).as("route-b producer ID").isNotEqualTo(RecordBatch.NO_PRODUCER_ID);

        var producesToA = routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE);
        var producesToB = routingCaptor.requestsToRoute("route-b", ApiKeys.PRODUCE);

        // All produces to each route use the same PID — proves mapping survived reconnect
        producesToA.forEach(event -> RoutingRequestEventAssert.assertThat(event).hasProducerId(pidA));
        producesToB.forEach(event -> RoutingRequestEventAssert.assertThat(event).hasProducerId(pidB));
    }

    @Test
    void shouldRecoverAfterProducerIdEviction() throws Exception {
        String topicA = "a.eviction";
        String topicB = "b.eviction";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);

        var config = topicRouterConfig(clusterA, clusterB, Duration.ofSeconds(2));

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", true,
                        "retries", 5,
                        "batch.size", 0,
                        "linger.ms", 0))) {

            // Phase 1: produce to both routes — establishes per-route PID mappings
            producer.send(new ProducerRecord<>(topicA, "k1", "before-a"))
                    .get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicB, "k1", "before-b"))
                    .get(10, TimeUnit.SECONDS);

            // Phase 2: wait for TTL to expire
            Thread.sleep(3000);

            // Phase 3: produce again — mapping evicted, producer re-initialises and retries
            producer.send(new ProducerRecord<>(topicA, "k2", "after-a"))
                    .get(30, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicB, "k2", "after-b"))
                    .get(30, TimeUnit.SECONDS);
        }

        // No duplicates on either backend, in order
        var recordsA = consumeDirectly(clusterA, topicA);
        var recordsB = consumeDirectly(clusterB, topicB);
        assertThat(recordsA).extracting(ConsumerRecord::value)
                .containsExactly("before-a", "after-a");
        assertThat(recordsB).extracting(ConsumerRecord::value)
                .containsExactly("before-b", "after-b");
    }

    // --- Version sweep ---

    static List<Arguments> produceVersions() {
        var result = new ArrayList<Arguments>();
        // TopicProduceData can't be serialised at v0–v2 (schema structure changed at v3)
        for (short v = 3; v <= 12; v++) {
            result.add(Arguments.of(v));
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource("produceVersions")
    void produceAcrossVersions(short apiVersion) throws Exception {
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            MetadataResponseData metadata;
            try (var metadataClient = tester.simpleTestClient()) {
                negotiateApiVersions(metadataClient);
                metadata = fetchMetadata(metadataClient, PARAM_TOPIC_A, PARAM_TOPIC_B);
            }

            produceToLeader(tester, metadata, PARAM_TOPIC_A, apiVersion);
            produceToLeader(tester, metadata, PARAM_TOPIC_B, apiVersion);
        }

        var producesToA = routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE);
        var producesToB = routingCaptor.requestsToRoute("route-b", ApiKeys.PRODUCE);
        assertThat(producesToA).as("v%d: PRODUCE should route to route-a", apiVersion).isNotEmpty();
        assertThat(producesToB).as("v%d: PRODUCE should route to route-b", apiVersion).isNotEmpty();

        for (var event : producesToA) {
            var body = (ProduceRequestData) event.body();
            assertThat(body.topicData()).extracting("name")
                    .as("v%d: route-a should only receive a.* topics", apiVersion)
                    .allSatisfy(name -> assertThat((String) name).startsWith("a."));
        }
        for (var event : producesToB) {
            var body = (ProduceRequestData) event.body();
            assertThat(body.topicData()).extracting("name")
                    .as("v%d: route-b should only receive b.* topics", apiVersion)
                    .allSatisfy(name -> assertThat((String) name).startsWith("b."));
        }
    }

    private void produceToLeader(KroxyliciousTester tester,
                                 MetadataResponseData metadata,
                                 String topicName,
                                 short apiVersion) {
        try (var client = clientForLeader(tester, metadata, topicName, 0)) {
            var request = new ProduceRequestData()
                    .setAcks((short) -1)
                    .setTimeoutMs(10_000);
            var topicData = new ProduceRequestData.TopicProduceData().setName(topicName);
            topicData.partitionData().add(new ProduceRequestData.PartitionProduceData()
                    .setIndex(0)
                    .setRecords(buildSingleRecord("key", "val")));
            request.topicData().add(topicData);

            var response = client.getSync(
                    new Request(ApiKeys.PRODUCE, apiVersion, "test-client", request));
            var body = (ProduceResponseData) response.payload().message();

            assertThat(body.responses()).extracting(ProduceResponseData.TopicProduceResponse::name)
                    .as("v%d response should contain %s", apiVersion, topicName)
                    .containsExactly(topicName);
            for (var topicResp : body.responses()) {
                for (var partResp : topicResp.partitionResponses()) {
                    assertThat(partResp.errorCode())
                            .as("v%d partition error for %s", apiVersion, topicResp.name())
                            .isEqualTo(Errors.NONE.code());
                }
            }
        }
    }

    // --- Helpers ---

    private static long initProducerIdFromResponse(RoutingEventCaptor captor,
                                                   String route) {
        return captor.responseEvents().stream()
                .filter(e -> e.route().equals(route))
                .filter(e -> e.apiKey() == ApiKeys.INIT_PRODUCER_ID)
                .map(e -> (InitProducerIdResponseData) e.body())
                .filter(r -> Errors.forCode(r.errorCode()) == Errors.NONE)
                .mapToLong(InitProducerIdResponseData::producerId)
                .reduce((first, last) -> last)
                .orElseThrow(() -> new AssertionError(
                        "No successful INIT_PRODUCER_ID response captured for route " + route));
    }

    private static MemoryRecords buildSingleRecord(String key, String value) {
        var builder = MemoryRecords.builder(
                java.nio.ByteBuffer.allocate(1024),
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0);
        builder.append(new SimpleRecord(System.currentTimeMillis(), key.getBytes(), value.getBytes()));
        return builder.build();
    }
}
