/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;
import io.micrometer.core.instrument.Metrics;
import io.netty.handler.codec.http.HttpResponseStatus;

import io.kroxylicious.proxy.config.MicrometerDefinitionBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.micrometer.CommonTagsHook;
import io.kroxylicious.proxy.micrometer.StandardBindersHook;
import io.kroxylicious.test.tester.SimpleMetric;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class MetricsIT {

    private static final String TOPIC_1 = "my-test-topic";

    @BeforeEach
    public void beforeEach() {
        assertThat(Metrics.globalRegistry.getMeters()).isEmpty();
    }

    @AfterEach
    public void afterEach() {
        assertThat(Metrics.globalRegistry.getMeters()).isEmpty();
    }

    @Test
    void nonexistentEndpointGives404(KafkaCluster cluster) {
        var config = proxy(cluster)
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getManagementClient()) {
            var notFoundResp = ahc.getFromAdminEndpoint("nonexistent");
            assertThat(notFoundResp.statusCode())
                    .isEqualTo(HttpResponseStatus.NOT_FOUND.code());
        }
    }

    @Test
    void scrapeEndpointExists(KafkaCluster cluster) {
        var config = proxy(cluster)
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

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
        var config = proxy(cluster)
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getManagementClient()) {
            var counterName = getRandomCounterName();
            Metrics.counter(counterName).increment();
            var metrics = ahc.scrapeMetrics();
            assertThat(metrics)
                    .hasSizeGreaterThan(0)
                    .extracting(SimpleMetric::name, SimpleMetric::value)
                    .contains(tuple(counterName, 1.0));
        }
    }

    @Test
    void prometheusMetricFromNamedBinder(KafkaCluster cluster) {
        var config = proxy(cluster)
                .addToMicrometer(
                        new MicrometerDefinitionBuilder(StandardBindersHook.class.getName()).withConfig("binderNames", List.of("JvmGcMetrics")).build())
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getManagementClient()) {
            assertThat(ahc.scrapeMetrics())
                    .hasSizeGreaterThan(0)
                    .extracting(SimpleMetric::name)
                    .contains("jvm_gc_memory_allocated_bytes_total");
        }
    }

    @Test
    void prometheusMetricsWithCommonTags(KafkaCluster cluster) {
        var config = proxy(cluster)
                .addToMicrometer(new MicrometerDefinitionBuilder(CommonTagsHook.class.getName()).withConfig("commonTags", Map.of("a", "b")).build())
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getManagementClient()) {
            var counterName = getRandomCounterName();
            Metrics.counter(counterName).increment();

            var metrics = ahc.scrapeMetrics();
            assertThat(metrics)
                    .filteredOn("name", counterName)
                    .singleElement()
                    .extracting(SimpleMetric::labels)
                    .isEqualTo(Map.of("a", "b"));
        }
    }

    @Test
    void shouldIncrementDownstreamMessagesOnProduceRequestWithoutFilter(KafkaCluster cluster, Topic topic) throws ExecutionException, InterruptedException {
        var config = proxy(cluster)
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

        // Given
        try (var tester = kroxyliciousTester(config);
                var managementClient = tester.getManagementClient();
                var producer = tester.producer()) {
            var metricList = managementClient.scrapeMetrics();
            var inboundDownstreamMessagesMetricsValue = getMetricsValue(metricList, "kroxylicious_inbound_downstream_messages_total", null);
            var inboundDownstreamDecodedMessagesMetricsValue = getMetricsValue(metricList, "kroxylicious_inbound_downstream_decoded_messages_total", null);

            // When
            producer.send(new ProducerRecord<>(topic.name(), "my-key", "hello-world")).get();

            // Then
            // updated metrics after some message were produced
            var updatedMetricsList = managementClient.scrapeMetrics();
            var updatedInboundDownstreamMessagesMetricsValue = getMetricsValue(updatedMetricsList, "kroxylicious_inbound_downstream_messages_total", null);
            var updatedInboundDownstreamDecodedMessagesMetricsValue = getMetricsValue(updatedMetricsList, "kroxylicious_inbound_downstream_decoded_messages_total", null);
            assertThat(updatedInboundDownstreamMessagesMetricsValue).isGreaterThan(inboundDownstreamMessagesMetricsValue);
            assertThat(updatedInboundDownstreamDecodedMessagesMetricsValue).isGreaterThan(inboundDownstreamDecodedMessagesMetricsValue);
        }
    }

    @Test
    void shouldIncrementDownstreamMessagesOnProduceRequestWithFilter(KafkaCluster cluster, Topic topic) throws ExecutionException, InterruptedException {

        // the downstream messages and decoded messages is not yet differentiated by ApiKey
        final UUID configInstance = UUID.randomUUID();

        NamedFilterDefinition namedFilterDefinition = new NamedFilterDefinitionBuilder("filter",
                CreateTopicRequest.class.getName()).withConfig("configInstanceId", configInstance).build();

        var config = proxy(cluster)
                .addToFilterDefinitions(namedFilterDefinition)
                .addToDefaultFilters(namedFilterDefinition.name())
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

        // Given
        try (var tester = kroxyliciousTester(config);
                var managementClient = tester.getManagementClient();
                var producer = tester.producer()) {
            var metricList = managementClient.scrapeMetrics();
            var inboundDownstreamMessagesMetricsValue = getMetricsValue(metricList, "kroxylicious_inbound_downstream_messages_total", null);
            var inboundDownstreamDecodedMessagesMetricsValue = getMetricsValue(metricList, "kroxylicious_inbound_downstream_decoded_messages_total", null);

            // When
            producer.send(new ProducerRecord<>(topic.name(), "my-key", "hello-world")).get();

            // Then
            // updated metrics after some message were produced
            var updatedMetricsList = managementClient.scrapeMetrics();

            var updatedInboundDownstreamMessagesMetricsValue = getMetricsValue(updatedMetricsList, "kroxylicious_inbound_downstream_messages_total", null);
            var updatedInboundDownstreamDecodedMessagesMetricsValue = getMetricsValue(updatedMetricsList, "kroxylicious_inbound_downstream_decoded_messages_total", null);

            assertThat(updatedInboundDownstreamMessagesMetricsValue).isGreaterThan(inboundDownstreamMessagesMetricsValue);
            assertThat(updatedInboundDownstreamDecodedMessagesMetricsValue).isGreaterThan(inboundDownstreamDecodedMessagesMetricsValue);
        }
    }

    @Test
    void shouldIncrementConnectionMetrics(KafkaCluster cluster, Topic topic) throws ExecutionException, InterruptedException {
        var config = proxy(cluster)
                .addToFilterDefinitions()
                .addToDefaultFilters()
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

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
        var config = proxy(cluster)
                .addToFilterDefinitions()
                .addToDefaultFilters()
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

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

    void assertMetricsDoesNotExist(List<SimpleMetric> metricList, String metricsName, ApiKeys apiKey) {
        assertThat(metricList)
                .hasSizeGreaterThan(0)
                .noneSatisfy(simpleMetric -> {
                    assertThat(simpleMetric.name()).isEqualTo(metricsName);
                    if (apiKey != null) {
                        assertThat(simpleMetric.labels()).containsValue(apiKey.toString());
                    }
                });
    }

    void assertMetricsWithValue(List<SimpleMetric> metricList, String metricsName, ApiKeys apiKey) {
        assertThat(metricList)
                .hasSizeGreaterThan(0)
                .anySatisfy(simpleMetric -> {
                    assertThat(simpleMetric.name()).isEqualTo(metricsName);
                    if (apiKey != null) {
                        assertThat(simpleMetric.labels()).containsValue(apiKey.toString());
                    }
                    assertThat(simpleMetric.value()).isGreaterThan(0);
                });
    }

    double getMetricsValue(List<SimpleMetric> metricList, String metricsName, ApiKeys apiKey) {

        if (apiKey != null) {
            return metricList.stream().filter(simpleMetric -> simpleMetric.name().equals(metricsName)
                    && simpleMetric.labels().containsValue(apiKey.toString())).findFirst().get().value();
        }
        else {
            return metricList.stream().filter(simpleMetric -> simpleMetric.name().equals(metricsName)).findFirst().get().value();
        }
    }

    @NonNull
    private String getRandomCounterName() {
        return "test_metric_" + Math.abs(new Random().nextLong()) + "_total";
    }
}
