/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.router.topic.TopicPartitionRouterFactory;
import io.kroxylicious.proxy.router.topic.config.RouteConfig;
import io.kroxylicious.proxy.router.topic.config.TopicPartitionRouterConfig;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils;
import io.kroxylicious.testing.integration.tester.KroxyliciousTester;

import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that topicId resolution works across independent proxy instances.
 * Exercises the enrichment filter cache-miss path where a proxy must
 * resolve topicIds it has never seen via internal METADATA requests.
 */
class CrossInstanceRoutingIT extends TopicPartitionRoutingBaseIT {

    /**
     * Two independent clients (producer and consumer) connecting to
     * different proxy instances. The consumer's proxy has cold caches
     * and must resolve topicIds independently.
     */
    @Test
    void shouldProduceAndConsumeAcrossTwoProxyInstances() throws Exception {
        String topicA = "a.cross-instance";
        String topicB = "b.cross-instance";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);

        var config1 = topicRouterConfigOnPort("localhost:19192");
        var config2 = topicRouterConfigOnPort("localhost:19292");

        try (var proxy1 = kroxyliciousTester(config1);
                var proxy2 = kroxyliciousTester(config2)) {

            try (var producer = proxy1.producer(Map.of(
                    "enable.idempotence", false,
                    "retries", 0,
                    "batch.size", 0,
                    "linger.ms", 0))) {
                producer.send(new ProducerRecord<>(topicA, "key-a", "val-a"))
                        .get(10, TimeUnit.SECONDS);
                producer.send(new ProducerRecord<>(topicB, "key-b", "val-b"))
                        .get(10, TimeUnit.SECONDS);
            }

            var recordsA = consumeViaProxy(proxy2, topicA);
            var recordsB = consumeViaProxy(proxy2, topicB);

            assertThat(recordsA).extracting(ConsumerRecord::value)
                    .containsExactly("val-a");
            assertThat(recordsB).extracting(ConsumerRecord::value)
                    .containsExactly("val-b");
        }
    }

    /**
     * A raw protocol client learns topicIds from one proxy then sends
     * a PRODUCE request with those topicIds to a different proxy whose
     * caches are completely cold. The second proxy must resolve the
     * topicIds via internal METADATA before it can route the request.
     */
    @Test
    void shouldHandleTopicIdLearnedFromDifferentProxy() throws Exception {
        String topicA = "a.cross-id";
        createTopic(topicA, clusterA);

        var config1 = topicRouterConfigOnPort("localhost:19392");
        var config2 = topicRouterConfigOnPort("localhost:19492");

        try (var proxy1 = kroxyliciousTester(config1);
                var proxy2 = kroxyliciousTester(config2)) {

            Uuid topicId;
            try (var client1 = proxy1.simpleTestClient()) {
                negotiateApiVersions(client1);
                MetadataResponseData metadata = fetchMetadata(client1, topicA);
                var topicMd = metadata.topics().stream()
                        .filter(t -> t.name().equals(topicA))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("topic not in metadata"));
                topicId = topicMd.topicId();
                assertThat(topicId).isNotEqualTo(Uuid.ZERO_UUID);
            }

            try (var client2 = proxy2.simpleTestClient()) {
                negotiateApiVersions(client2);

                var produceRequest = new ProduceRequestData()
                        .setAcks((short) -1)
                        .setTimeoutMs(10_000);
                var topicData = new ProduceRequestData.TopicProduceData()
                        .setTopicId(topicId);
                topicData.partitionData().add(
                        new ProduceRequestData.PartitionProduceData()
                                .setIndex(0)
                                .setRecords(buildSingleRecord("cross-key", "cross-val")));
                produceRequest.topicData().add(topicData);

                var response = client2.getSync(
                        new Request(ApiKeys.PRODUCE, (short) 13, "test-client", produceRequest));
                var body = (ProduceResponseData) response.payload().message();

                assertThat(body.responses()).isNotEmpty();
                for (var topicResp : body.responses()) {
                    for (var partResp : topicResp.partitionResponses()) {
                        assertThat(partResp.errorCode())
                                .as("partition %d error", partResp.index())
                                .isEqualTo(Errors.NONE.code());
                    }
                }
            }

            var records = consumeDirectly(clusterA, topicA);
            assertThat(records).extracting(ConsumerRecord::value)
                    .containsExactly("cross-val");
        }
    }

    private ConfigurationBuilder topicRouterConfigOnPort(String bootstrapAddress) {
        var targetA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var targetB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);

        var routeA = new RouteDefinition("route-a", 0, null, new RouteTarget("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1, null, new RouteTarget("cluster-b", null));

        var routerConfig = new TopicPartitionRouterConfig(
                "route-a",
                List.of(new RouteConfig("route-a", List.of("a.")),
                        new RouteConfig("route-b", List.of("b."))),
                null, null, null);

        var routerDef = new RouterDefinition("topic-router",
                TopicPartitionRouterFactory.class.getName(),
                routerConfig, List.of(routeA, routeB));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "topic-router"))
                .addToGateways(KroxyliciousConfigUtils
                        .defaultPortIdentifiesNodeGatewayBuilder(bootstrapAddress)
                        .editPortIdentifiesNode()
                        .addToNodeIdRanges(new NamedRange("brokers", 0, 5))
                        .endPortIdentifiesNode()
                        .build())
                .build();

        return KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToClusterDefinitions(targetA, targetB)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }

    private List<ConsumerRecord<String, String>> consumeViaProxy(
                                                                 KroxyliciousTester tester,
                                                                 String topic) {
        var props = new java.util.HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, tester.getBootstrapAddress());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "cross-instance-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(Set.of(topic));
            List<ConsumerRecord<String, String>> all = new ArrayList<>();
            int consecutiveEmpty = 0;
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline && consecutiveEmpty < 3) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
                batch.forEach(all::add);
                if (batch.isEmpty() && !all.isEmpty()) {
                    consecutiveEmpty++;
                }
                else if (!batch.isEmpty()) {
                    consecutiveEmpty = 0;
                }
            }
            return all;
        }
    }

    private static MemoryRecords buildSingleRecord(String key, String value) {
        var builder = MemoryRecords.builder(
                ByteBuffer.allocate(1024),
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0);
        builder.append(new SimpleRecord(System.currentTimeMillis(), key.getBytes(), value.getBytes()));
        return builder.build();
    }
}
