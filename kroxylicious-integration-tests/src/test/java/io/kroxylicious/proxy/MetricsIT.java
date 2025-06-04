/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.protocol.ApiKeys;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;
import io.micrometer.core.instrument.Metrics;
import io.netty.handler.codec.http.HttpResponseStatus;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.MicrometerDefinitionBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.micrometer.CommonTagsHook;
import io.kroxylicious.proxy.micrometer.StandardBindersHook;
import io.kroxylicious.test.tester.SimpleMetric;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;
import io.kroxylicious.testing.kafka.junit5ext.TopicPartitions;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.proxy.internal.util.Metrics.API_KEY_LABEL;
import static io.kroxylicious.proxy.internal.util.Metrics.DECODED_LABEL;
import static io.kroxylicious.proxy.internal.util.Metrics.NODE_ID_LABEL;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static io.kroxylicious.test.tester.SimpleMetricAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class MetricsIT {

    @BeforeEach
    void beforeEach() {
        assertThat(Metrics.globalRegistry.getMeters()).isEmpty();
    }

    @AfterEach
    void afterEach() {
        assertThat(Metrics.globalRegistry.getMeters()).isEmpty();
    }

    @Test
    void nonexistentEndpointGives404(KafkaCluster cluster) {
        var config = configWithMetrics(cluster);

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getManagementClient()) {
            var notFoundResp = ahc.getFromAdminEndpoint("nonexistent");
            assertThat(notFoundResp.statusCode())
                    .isEqualTo(HttpResponseStatus.NOT_FOUND.code());
        }
    }

    @Test
    void scrapeEndpointExists(KafkaCluster cluster) {
        var config = configWithMetrics(cluster);

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getManagementClient()) {
            var ok = ahc.getFromAdminEndpoint("metrics");
            assertThat(ok.statusCode())
                    .isEqualTo(HttpResponseStatus.OK.code());
            assertThat(ok.body())
                    .isNotEmpty();
        }
    }

    @Test
    void knownPrometheusMetricPresent(KafkaCluster cluster) {
        var config = configWithMetrics(cluster);

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getManagementClient()) {
            var counterName = getRandomCounterName();
            Metrics.counter(counterName).increment();
            var metrics = ahc.scrapeMetrics();
            assertThat(metrics)
                    .filterByName(counterName)
                    .singleElement()
                    .value()
                    .isEqualTo(1.0);
        }
    }

    @Test
    void prometheusMetricFromNamedBinder(KafkaCluster cluster) {
        var config = configWithMetrics(cluster)
                .addToMicrometer(
                        new MicrometerDefinitionBuilder(StandardBindersHook.class.getName()).withConfig("binderNames", List.of("JvmGcMetrics")).build());

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getManagementClient()) {
            assertThat(ahc.scrapeMetrics())
                    .filterByName("jvm_gc_memory_allocated_bytes_total")
                    .singleElement()
                    .isNotNull();
        }
    }

    @Test
    void prometheusMetricsWithCommonTags(KafkaCluster cluster) {
        var config = configWithMetrics(cluster)
                .addToMicrometer(new MicrometerDefinitionBuilder(CommonTagsHook.class.getName()).withConfig("commonTags", Map.of("a", "b")).build());

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getManagementClient()) {
            var counterName = getRandomCounterName();
            Metrics.counter(counterName).increment();

            var metrics = ahc.scrapeMetrics();
            assertThat(metrics)
                    .filterByName(counterName)
                    .singleElement()
                    .labels()
                    .containsEntry("a", "b");
        }
    }

    static Stream<Arguments> transitScenarios() {
        var decodeAll = new NamedFilterDefinitionBuilder("decodeAll", "DecodeAll").build();
        var rejectCreateTopic = new NamedFilterDefinitionBuilder("rejectCreateTopic", "RejectingCreateTopicFilterFactory").withConfig("respondWithError", Boolean.FALSE)
                .build();
        return Stream.of(
                argumentSet("counts opaque requests from client",
                        (UnaryOperator<ConfigurationBuilder>) builder -> builder,
                        (Consumer<List<SimpleMetric>>) metricList -> assertThat(metricList)
                                .filterByName("kroxylicious_client_to_proxy_request_total")
                                .filterByTag(API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name())
                                .filterByTag(NODE_ID_LABEL, "0")
                                .filterByTag(DECODED_LABEL, "false")
                                .isEmpty(),
                        (Consumer<List<SimpleMetric>>) metricList -> assertThat(metricList)
                                .filterByName("kroxylicious_client_to_proxy_request_total")
                                .filterByTag(API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name())
                                .filterByTag(NODE_ID_LABEL, "0")
                                .filterByTag(DECODED_LABEL, "false")
                                .singleElement()
                                .value()
                                .isEqualTo(1.0)),
                argumentSet("counts decoded requests from client",
                        (UnaryOperator<ConfigurationBuilder>) builder -> addFilterToConfig(builder, decodeAll),
                        (Consumer<List<SimpleMetric>>) metricList -> assertThat(metricList)
                                .filterByName("kroxylicious_client_to_proxy_request_total")
                                .filterByTag(API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name())
                                .filterByTag(NODE_ID_LABEL, "0")
                                .isEmpty(),
                        (Consumer<List<SimpleMetric>>) metricList -> assertThat(metricList)
                                .filterByName("kroxylicious_client_to_proxy_request_total")
                                .filterByTag(API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name())
                                .filterByTag(NODE_ID_LABEL, "0")
                                .filterByTag(DECODED_LABEL, "true")
                                .singleElement()
                                .value()
                                .isEqualTo(1.0)),
                argumentSet("counts opaque requests to server",
                        (UnaryOperator<ConfigurationBuilder>) builder -> builder,
                        (Consumer<List<SimpleMetric>>) metricList -> assertThat(metricList)
                                .filterByName("kroxylicious_proxy_to_server_request_total")
                                .filterByTag(API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name())
                                .filterByTag(NODE_ID_LABEL, "0")
                                .filterByTag(DECODED_LABEL, "false")
                                .isEmpty(),
                        (Consumer<List<SimpleMetric>>) metricList -> assertThat(metricList)
                                .filterByName("kroxylicious_proxy_to_server_request_total")
                                .filterByTag(API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name())
                                .filterByTag(NODE_ID_LABEL, "0")
                                .filterByTag(DECODED_LABEL, "false")
                                .singleElement()
                                .value()
                                .isEqualTo(1.0)),
                argumentSet("counts decoded requests to server",
                        (UnaryOperator<ConfigurationBuilder>) builder -> addFilterToConfig(builder, decodeAll),
                        (Consumer<List<SimpleMetric>>) metricList -> assertThat(metricList)
                                .filterByName("kroxylicious_proxy_to_server_request_total")
                                .filterByTag(API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name())
                                .filterByTag(NODE_ID_LABEL, "0")
                                .filterByTag(DECODED_LABEL, "true")
                                .isEmpty(),
                        (Consumer<List<SimpleMetric>>) metricList -> assertThat(metricList)
                                .filterByName("kroxylicious_proxy_to_server_request_total")
                                .filterByTag(API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name())
                                .filterByTag(NODE_ID_LABEL, "0")
                                .filterByTag(DECODED_LABEL, "true")
                                .singleElement()
                                .value()
                                .isEqualTo(1.0)),
                argumentSet("counts bootstrap requests",
                        (UnaryOperator<ConfigurationBuilder>) builder -> builder,
                        (Consumer<List<SimpleMetric>>) metricList -> {
                        },
                        (Consumer<List<SimpleMetric>>) metricList -> {
                            assertThat(metricList)
                                    .filterByName("kroxylicious_client_to_proxy_request_total")
                                    .filterByTag(API_KEY_LABEL, ApiKeys.METADATA.name())
                                    .filterByTag(NODE_ID_LABEL, "bootstrap")
                                    .singleElement()
                                    .value()
                                    .isGreaterThanOrEqualTo(1.0);
                            assertThat(metricList)
                                    .filterByName("kroxylicious_proxy_to_server_request_total")
                                    .filterByTag(API_KEY_LABEL, ApiKeys.METADATA.name())
                                    .filterByTag(NODE_ID_LABEL, "bootstrap")
                                    .singleElement()
                                    .value()
                                    .isGreaterThanOrEqualTo(1.0);
                        }),
                argumentSet("counts opaque responses from server",
                        (UnaryOperator<ConfigurationBuilder>) builder -> builder,
                        (Consumer<List<SimpleMetric>>) metricList -> assertThat(metricList)
                                .filterByName("kroxylicious_server_to_proxy_response_total")
                                .filterByTag(API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name())
                                .filterByTag(NODE_ID_LABEL, "0")
                                .filterByTag(DECODED_LABEL, "false")
                                .isEmpty(),
                        (Consumer<List<SimpleMetric>>) metricList -> assertThat(metricList)
                                .filterByName("kroxylicious_server_to_proxy_response_total")
                                .filterByTag(API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name())
                                .filterByTag(NODE_ID_LABEL, "0")
                                .filterByTag(DECODED_LABEL, "false")
                                .singleElement()
                                .value()
                                .isEqualTo(1.0)),
                argumentSet("counts decoded responses to client",
                        (UnaryOperator<ConfigurationBuilder>) builder -> addFilterToConfig(builder, decodeAll),
                        (Consumer<List<SimpleMetric>>) metricList -> assertThat(metricList)
                                .filterByName("kroxylicious_proxy_to_client_response_total")
                                .filterByTag(API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name())
                                .filterByTag(NODE_ID_LABEL, "0")
                                .filterByTag(DECODED_LABEL, "true")
                                .isEmpty(),
                        (Consumer<List<SimpleMetric>>) metricList -> assertThat(metricList)
                                .filterByName("kroxylicious_proxy_to_client_response_total")
                                .filterByTag(API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name())
                                .filterByTag(NODE_ID_LABEL, "0")
                                .filterByTag(DECODED_LABEL, "true")
                                .singleElement()
                                .value()
                                .isEqualTo(1.0)),
                argumentSet("short circuited request does not tick upstream",
                        (UnaryOperator<ConfigurationBuilder>) builder -> addFilterToConfig(builder, rejectCreateTopic),
                        (Consumer<List<SimpleMetric>>) metricList -> {
                        },
                        (Consumer<List<SimpleMetric>>) metricList -> {
                            assertThat(metricList)
                                    .filterByName("kroxylicious_client_to_proxy_request_total")
                                    .filterByTag(API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name())
                                    .singleElement()
                                    .value()
                                    .isEqualTo(1.0);
                            assertThat(metricList)
                                    .filterByName("kroxylicious_proxy_to_client_response_total")
                                    .filterByTag(API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name())
                                    .singleElement()
                                    .value()
                                    .isEqualTo(1.0);
                            assertThat(metricList)
                                    .describedAs("create topic should not have gone upstream")
                                    .filterByTag(API_KEY_LABEL, ApiKeys.CREATE_TOPICS.name())
                                    .extracting(SimpleMetric::name)
                                    .doesNotContain("kroxylicious_proxy_to_server_request_total", "kroxylicious_server_to_proxy_response_total");
                        })

        );
    }

    @ParameterizedTest
    @MethodSource("transitScenarios")
    void shouldIncrementCountOnMessageTransit(UnaryOperator<ConfigurationBuilder> builder,
                                              Consumer<List<SimpleMetric>> beforeAssertion,
                                              Consumer<List<SimpleMetric>> afterAssertion,
                                              KafkaCluster cluster) {
        var config = configWithMetrics(cluster);

        // Given
        try (var tester = kroxyliciousTester(builder.apply(config));
                var managementClient = tester.getManagementClient();
                var admin = tester.admin()) {
            assertThat(admin.describeCluster().clusterId()).succeedsWithin(Duration.ofSeconds(5));

            var metricList = managementClient.scrapeMetrics();
            beforeAssertion.accept(metricList);

            // When
            var future = admin.createTopics(List.of(new NewTopic(UUID.randomUUID().toString(), Optional.empty(), Optional.empty()))).all();

            // Then
            assertThat(future).succeedsWithin(Duration.ofSeconds(5));
            metricList = managementClient.scrapeMetrics();
            afterAssertion.accept(metricList);
        }
    }

    @Test
    void countMetricsShouldBeDiscriminatedByNodeId(@BrokerCluster(numBrokers = 2) KafkaCluster cluster, @TopicPartitions(2) Topic topic)
            throws ExecutionException, InterruptedException {
        var config = configWithMetrics(cluster);

        try (var tester = kroxyliciousTester(config);
                var managementClient = tester.getManagementClient();
                var producer = tester.producer();
                var admin = tester.admin()) {

            // Given
            var describeFuture = admin.describeTopics(List.of(topic.name())).allTopicNames();
            assertThat(describeFuture).succeedsWithin(Duration.ofSeconds(5));

            var partitions = describeFuture.get().get(topic.name()).partitions();
            var partitionZeroNodeId = getNodeIdForPartition(partitions, 0);
            var partitionOneNodeId = getNodeIdForPartition(partitions, 1);
            assertThat(partitionZeroNodeId)
                    .describedAs("expecting topic partitions to be distributed amongst brokers")
                    .isNotEqualTo(partitionOneNodeId);

            // When
            List.of(new ProducerRecord<>(topic.name(), 0, "my-key", "0-1"),
                    new ProducerRecord<>(topic.name(), 0, "my-key", "0-2"),
                    new ProducerRecord<>(topic.name(), 1, "my-key", "0-2"))
                    .forEach(pr -> assertThat(producer.send(pr)).succeedsWithin(Duration.ofSeconds(5)));

            // Then
            var metricList = managementClient.scrapeMetrics();

            assertThat(metricList)
                    .filterByName("kroxylicious_client_to_proxy_request_total")
                    .filterByTag(API_KEY_LABEL, ApiKeys.PRODUCE.name())
                    .filterByTag(NODE_ID_LABEL, "" + partitionZeroNodeId)
                    .singleElement()
                    .value()
                    .isEqualTo(2.0);

            assertThat(metricList)
                    .filterByName("kroxylicious_client_to_proxy_request_total")
                    .filterByTag(API_KEY_LABEL, ApiKeys.PRODUCE.name())
                    .filterByTag(NODE_ID_LABEL, "" + partitionOneNodeId)
                    .singleElement()
                    .value()
                    .isEqualTo(1.0);
        }
    }

    @Test
    @Deprecated(since = "0.13.0", forRemoval = true)
    void shouldIncrementDownstreamMessagesOnProduceRequestWithoutFilter(KafkaCluster cluster, Topic topic) throws ExecutionException, InterruptedException {
        var config = configWithMetrics(cluster);

        // Given
        try (var tester = kroxyliciousTester(config);
                var managementClient = tester.getManagementClient();
                var producer = tester.producer()) {
            var metricList = managementClient.scrapeMetrics();
            var inboundDownstreamMessagesMetricsValue = getMetricsValue(metricList, "kroxylicious_inbound_downstream_messages_total");
            var inboundDownstreamDecodedMessagesMetricsValue = getMetricsValue(metricList, "kroxylicious_inbound_downstream_decoded_messages_total");

            // When
            producer.send(new ProducerRecord<>(topic.name(), "my-key", "hello-world")).get();

            // Then
            // updated metrics after some message were produced
            var updatedMetricsList = managementClient.scrapeMetrics();
            var updatedInboundDownstreamMessagesMetricsValue = getMetricsValue(updatedMetricsList, "kroxylicious_inbound_downstream_messages_total");
            var updatedInboundDownstreamDecodedMessagesMetricsValue = getMetricsValue(updatedMetricsList, "kroxylicious_inbound_downstream_decoded_messages_total");
            assertThat(updatedInboundDownstreamMessagesMetricsValue).isGreaterThan(inboundDownstreamMessagesMetricsValue);
            assertThat(updatedInboundDownstreamDecodedMessagesMetricsValue).isGreaterThan(inboundDownstreamDecodedMessagesMetricsValue);
        }
    }

    @Test
    @Deprecated(since = "0.13.0", forRemoval = true)
    void shouldIncrementDownstreamMessagesOnProduceRequestWithFilter(KafkaCluster cluster, Topic topic) throws ExecutionException, InterruptedException {

        // the downstream messages and decoded messages is not yet differentiated by ApiKey
        final UUID configInstance = UUID.randomUUID();

        NamedFilterDefinition namedFilterDefinition = new NamedFilterDefinitionBuilder("filter",
                CreateTopicRequest.class.getName()).withConfig("configInstanceId", configInstance).build();

        var config = addFilterToConfig(configWithMetrics(cluster), namedFilterDefinition);

        // Given
        try (var tester = kroxyliciousTester(config);
                var managementClient = tester.getManagementClient();
                var producer = tester.producer()) {
            var metricList = managementClient.scrapeMetrics();
            var inboundDownstreamMessagesMetricsValue = getMetricsValue(metricList, "kroxylicious_inbound_downstream_messages_total");
            var inboundDownstreamDecodedMessagesMetricsValue = getMetricsValue(metricList, "kroxylicious_inbound_downstream_decoded_messages_total");

            // When
            producer.send(new ProducerRecord<>(topic.name(), "my-key", "hello-world")).get();

            // Then
            // updated metrics after some message were produced
            var updatedMetricsList = managementClient.scrapeMetrics();
            var updatedInboundDownstreamMessagesMetricsValue = getMetricsValue(updatedMetricsList, "kroxylicious_inbound_downstream_messages_total");
            var updatedInboundDownstreamDecodedMessagesMetricsValue = getMetricsValue(updatedMetricsList, "kroxylicious_inbound_downstream_decoded_messages_total");
            assertThat(updatedInboundDownstreamMessagesMetricsValue).isGreaterThan(inboundDownstreamMessagesMetricsValue);
            assertThat(updatedInboundDownstreamDecodedMessagesMetricsValue).isGreaterThan(inboundDownstreamDecodedMessagesMetricsValue);
        }
    }

    @Test
    void shouldIncrementConnectionMetrics(KafkaCluster cluster, Topic topic) throws ExecutionException, InterruptedException {
        var config = configWithMetrics(cluster);

        // Given
        try (var tester = kroxyliciousTester(config);
                var managementClient = tester.getManagementClient()) {
            var metricsList = managementClient.scrapeMetrics();
            assertMetricsDoesNotExist(metricsList, "kroxylicious_downstream_connections_total", null);
            assertMetricsDoesNotExist(metricsList, "kroxylicious_upstream_connections_total", null);

            // When
            var producer = tester.producer();
            producer.send(new ProducerRecord<>(topic.name(), "my-key", "hello-world")).get();

            // Then
            // updated metrics after some message were produced
            var updatedMetricsList = managementClient.scrapeMetrics();
            assertMetricsWithValue(updatedMetricsList, "kroxylicious_downstream_connections_total", null);
            assertMetricsWithValue(updatedMetricsList, "kroxylicious_upstream_connections_total", null);
        }
    }

    @Test
    void shouldIncrementPayloadSizeBytesMetricsOnProduceRequest(KafkaCluster cluster, Topic topic) throws ExecutionException, InterruptedException {
        var config = configWithMetrics(cluster);

        // Given
        try (var tester = kroxyliciousTester(config);
                var managementClient = tester.getManagementClient();
                var producer = tester.producer();) {
            var metricList = managementClient.scrapeMetrics();
            assertMetricsDoesNotExist(metricList, "kroxylicious_payload_size_bytes_count", ApiKeys.PRODUCE);
            assertMetricsDoesNotExist(metricList, "kroxylicious_payload_size_bytes_sum", ApiKeys.PRODUCE);

            // When
            producer.send(new ProducerRecord<>(topic.name(), "my-key", "hello-world")).get();
            producer.send(new ProducerRecord<>(topic.name(), "my-key", "hello-world")).get();

            // Then
            // updated metrics after some message were produced
            var updatedMetricList = managementClient.scrapeMetrics();
            assertMetricsWithValue(updatedMetricList, "kroxylicious_payload_size_bytes_count", ApiKeys.PRODUCE);
            assertMetricsWithValue(updatedMetricList, "kroxylicious_payload_size_bytes_sum", ApiKeys.PRODUCE);
        }
    }

    private int getNodeIdForPartition(List<TopicPartitionInfo> partitions, int partitionId) {
        return partitions.stream().filter(p -> p.partition() == partitionId).map(TopicPartitionInfo::leader).map(Node::id).findFirst().orElseThrow();
    }

    void assertMetricsDoesNotExist(List<SimpleMetric> metricList, String metricsName, ApiKeys apiKey) {
        Assertions.assertThat(metricList)
                .hasSizeGreaterThan(0)
                .noneSatisfy(simpleMetric -> {
                    assertThat(simpleMetric.name()).isEqualTo(metricsName);
                    if (apiKey != null) {
                        assertThat(simpleMetric.labels()).containsValue(apiKey.toString());
                    }
                });
    }

    void assertMetricsWithValue(List<SimpleMetric> metricList, String metricsName, ApiKeys apiKey) {
        Assertions.assertThat(metricList)
                .hasSizeGreaterThan(0)
                .anySatisfy(simpleMetric -> {
                    assertThat(simpleMetric.name()).isEqualTo(metricsName);
                    if (apiKey != null) {
                        assertThat(simpleMetric.labels()).containsValue(apiKey.toString());
                    }
                    assertThat(simpleMetric.value()).isGreaterThan(0);
                });
    }

    double getMetricsValue(List<SimpleMetric> metricList, String metricsName) {
        return getMetricsValue(metricList, metricsName, labels -> true);
    }

    double getMetricsValue(List<SimpleMetric> metricList, String metricsName, Predicate<Map<String, String>> labelPredicate) {
        return metricList.stream()
                .filter(simpleMetric -> simpleMetric.name().equals(metricsName))
                .filter(simpleMetric -> labelPredicate.test(simpleMetric.labels()))
                .findFirst()
                .orElseThrow().value();
    }

    @NonNull
    private String getRandomCounterName() {
        return "test_metric_" + Math.abs(new Random().nextLong()) + "_total";
    }

    private ConfigurationBuilder configWithMetrics(KafkaCluster cluster) {
        return proxy(cluster)
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();
    }

    private static ConfigurationBuilder addFilterToConfig(ConfigurationBuilder builder, NamedFilterDefinition filterDefinition) {
        return builder
                .addToFilterDefinitions(filterDefinition)
                .addToDefaultFilters(filterDefinition.name());
    }
}
