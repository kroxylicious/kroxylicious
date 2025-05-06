/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;
import io.micrometer.core.instrument.Metrics;
import io.netty.handler.codec.http.HttpResponseStatus;

import io.kroxylicious.proxy.config.MicrometerDefinitionBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.filter.simpletransform.ProduceRequestTransformation;
import io.kroxylicious.proxy.micrometer.CommonTagsHook;
import io.kroxylicious.proxy.micrometer.StandardBindersHook;
import io.kroxylicious.test.tester.SimpleMetric;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class MetricsIT {

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
    void testKroxyliciousInboundDownstreamMessages(KafkaCluster cluster, Topic topic) {

        NamedFilterDefinition namedFilterDefinition = new NamedFilterDefinitionBuilder("filter-1",
                ProduceRequestTransformation.class.getName())
                .withConfig("transformation", TestEncoderFactory.class.getName()).build();

        var config = proxy(cluster)
                .addToFilterDefinitions(namedFilterDefinition)
                .addToDefaultFilters(namedFilterDefinition.name())
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();


        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getManagementClient();
                var producer = tester.producer(Map.of(CLIENT_ID_CONFIG, "shouldModifyProduceMessage", DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
                var consumer = tester
                        .consumer(Serdes.String(), Serdes.ByteArray(), Map.of(GROUP_ID_CONFIG, "my-group-id", AUTO_OFFSET_RESET_CONFIG, "earliest"));
        ) {

            var metricList = ahc.scrapeMetrics();

            var inboundDownstreamMessagesMetricsValue = metricList.stream().filter(simpleMetric -> simpleMetric.name()
                    .equals("kroxylicious_inbound_downstream_messages_total")).findFirst().get().value();
            var inboundDownstreamDecodedMessagesMetricsValue = metricList.stream().filter(simpleMetric -> simpleMetric.name()
                    .equals("kroxylicious_inbound_downstream_decoded_messages_total")).findFirst().get().value();

            consumer.subscribe(Set.of(topic.name()));

            producer.send(new ProducerRecord<>(topic.name(), "my-key", "hello-world")).get();

            //updated metrics after some message were produced
            metricList = ahc.scrapeMetrics();

            var updatedInboundDownstreamMessagesMetricsValue = metricList.stream().filter(simpleMetric -> simpleMetric.name()
                    .equals("kroxylicious_inbound_downstream_messages_total")).findFirst().get().value();
            var updatedInboundDownstreamDecodedMessagesMetricsValue = metricList.stream().filter(simpleMetric -> simpleMetric.name()
                    .equals("kroxylicious_inbound_downstream_decoded_messages_total")).findFirst().get().value();

            Assertions.assertTrue(inboundDownstreamMessagesMetricsValue < updatedInboundDownstreamMessagesMetricsValue);
            Assertions.assertTrue(inboundDownstreamDecodedMessagesMetricsValue < updatedInboundDownstreamDecodedMessagesMetricsValue);

        }
        catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @NonNull
    private String getRandomCounterName() {
        return "test_metric_" + Math.abs(new Random().nextLong()) + "_total";
    }
}
