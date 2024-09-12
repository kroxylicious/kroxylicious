/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;
import io.micrometer.core.instrument.Metrics;
import io.netty.handler.codec.http.HttpResponseStatus;

import io.kroxylicious.proxy.config.MicrometerDefinitionBuilder;
import io.kroxylicious.proxy.micrometer.CommonTagsHook;
import io.kroxylicious.proxy.micrometer.StandardBindersHook;
import io.kroxylicious.test.tester.SimpleMetric;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
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
                                   .withNewAdminHttp()
                                   .withNewEndpoints()
                                   .withNewPrometheus()
                                   .endPrometheus()
                                   .endEndpoints()
                                   .endAdminHttp();

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getAdminHttpClient()) {
            var notFoundResp = ahc.getFromAdminEndpoint("nonexistent");
            assertThat(notFoundResp.statusCode())
                                                 .isEqualTo(HttpResponseStatus.NOT_FOUND.code());
        }
    }

    @Test
    void scrapeEndpointExists(KafkaCluster cluster) {
        var config = proxy(cluster)
                                   .withNewAdminHttp()
                                   .withNewEndpoints()
                                   .withNewPrometheus()
                                   .endPrometheus()
                                   .endEndpoints()
                                   .endAdminHttp();

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getAdminHttpClient()) {
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
                                   .withNewAdminHttp()
                                   .withNewEndpoints()
                                   .withNewPrometheus()
                                   .endPrometheus()
                                   .endEndpoints()
                                   .endAdminHttp();

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getAdminHttpClient()) {
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
                                           new MicrometerDefinitionBuilder(StandardBindersHook.class.getName()).withConfig("binderNames", List.of("JvmGcMetrics")).build()
                                   )
                                   .withNewAdminHttp()
                                   .withNewEndpoints()
                                   .withNewPrometheus()
                                   .endPrometheus()
                                   .endEndpoints()
                                   .endAdminHttp();

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getAdminHttpClient()) {
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
                                   .withNewAdminHttp()
                                   .withNewEndpoints()
                                   .withNewPrometheus()
                                   .endPrometheus()
                                   .endEndpoints()
                                   .endAdminHttp();

        try (var tester = kroxyliciousTester(config);
                var ahc = tester.getAdminHttpClient()) {
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

    @NonNull
    private String getRandomCounterName() {
        return "test_metric_" + Math.abs(new Random().nextLong()) + "_total";
    }
}
