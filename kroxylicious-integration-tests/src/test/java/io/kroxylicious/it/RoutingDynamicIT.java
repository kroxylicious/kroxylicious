/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.router.DynamicProduceRouterFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.testing.filter.record.RecordTestUtils;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests verifying that dynamic routing ({@code Router.onRequest()})
 * works correctly end-to-end: requests are deserialised, passed to the router,
 * forwarded to the backend via {@code sendRequest()}, and the response is
 * delivered to the client via {@code respondWith()}.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RoutingDynamicIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();

    private static final String ROUTE_NAME = "default-route";
    private static final String ROUTER_NAME = "dynamic-produce";
    private static final String TARGET_CLUSTER_NAME = "backing";

    private ConfigurationBuilder dynamicRoutingConfig(KafkaCluster cluster) {
        var targetCluster = new ClusterDefinition(TARGET_CLUSTER_NAME, cluster.getBootstrapServers(), null);
        var route = new RouteDefinition(ROUTE_NAME, 0, List.of(), new RouteTarget(TARGET_CLUSTER_NAME, null));
        var routerConfig = new DynamicProduceRouterFactory.Config(ROUTE_NAME);
        var routerDef = new RouterDefinition(ROUTER_NAME,
                DynamicProduceRouterFactory.class.getName(), routerConfig, List.of(route));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        return baseConfigurationBuilder()
                .addToClusterDefinitions(targetCluster)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }

    @Test
    void shouldForwardFireAndForgetProduceToUpstream(KafkaCluster cluster, Topic topic) {
        // Given
        var config = dynamicRoutingConfig(cluster);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of("acks", "0", "retries", "0", "linger.ms", "0"));
                var consumer = tester.consumer(
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "fire-and-forget-upstream-test",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            // When: acks=0 produce — send() returns before any broker acknowledgement
            producer.send(new ProducerRecord<>(topic.name(), "key", "value"));
            producer.flush();

            // Then: record arrives at the upstream Kafka cluster
            consumer.subscribe(Set.of(topic.name()));
            assertThat(consumer.poll(Duration.ofSeconds(10)).iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("value");
        }
    }

    @Test
    void shouldFireAndForgetProduceNotBlockSubsequentResponses(KafkaCluster cluster, Topic topic) {
        // Given
        var config = dynamicRoutingConfig(cluster);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var client = tester.simpleTestClient()) {

            // Establish the upstream route before sending PRODUCE requests
            // (METADATA is statically routed so it does not consume a response-sequencer slot)
            client.getSync(new Request(ApiKeys.METADATA, (short) 12, "test", new MetadataRequestData()));

            // When: fire-and-forget PRODUCE (acks=0) — dynamically routed, no response sent to client
            var fireAndForget = client.get(produceRequest(topic.name(), (short) 0));
            assertThat(fireAndForget).succeedsWithin(Duration.ofSeconds(5)).isNull();

            // Then: subsequent PRODUCE (acks=1) receives its response — sequencer was not blocked
            var withAck = client.getSync(produceRequest(topic.name(), (short) 1));
            assertThat(withAck.payload().message()).isInstanceOf(ProduceResponseData.class);
            var produceResponse = (ProduceResponseData) withAck.payload().message();
            assertThat(produceResponse.responses()).singleElement()
                    .satisfies(r -> assertThat(r.partitionResponses()).singleElement()
                            .satisfies(p -> assertThat(p.errorCode()).isEqualTo(Errors.NONE.code())));
        }
    }

    private static Request produceRequest(String topicName, short acks) {
        var records = RecordTestUtils.singleElementMemoryRecords("key", "value");
        var partitionData = new ProduceRequestData.PartitionProduceData()
                .setIndex(0)
                .setRecords(records);
        var topicData = new ProduceRequestData.TopicProduceData()
                .setName(topicName)
                .setPartitionData(List.of(partitionData));
        var topicCollection = new ProduceRequestData.TopicProduceDataCollection(
                List.of(topicData).iterator());
        var produceData = new ProduceRequestData()
                .setAcks(acks)
                .setTimeoutMs(5000)
                .setTopicData(topicCollection);
        return new Request(ApiKeys.PRODUCE, (short) 9, "test-client", produceData);
    }

    @Test
    void shouldProduceAndConsumeViaDynamicRouting(KafkaCluster cluster, Topic topic) {
        // Given
        var config = dynamicRoutingConfig(cluster);

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer();
                var consumer = tester.consumer(
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "dynamic-routing-test",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            // When
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "key", "value")))
                    .succeedsWithin(Duration.ofSeconds(10));

            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));

            // Then
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("value");
        }
    }
}
