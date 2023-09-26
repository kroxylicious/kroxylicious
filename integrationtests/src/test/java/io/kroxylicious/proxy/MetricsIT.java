/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.micrometer.core.instrument.Metrics;
import io.netty.handler.codec.http.HttpResponseStatus;

import io.kroxylicious.proxy.config.MicrometerDefinitionBuilder;
import io.kroxylicious.proxy.micrometer.CommonTagsHook;
import io.kroxylicious.proxy.micrometer.StandardBindersHook;
import io.kroxylicious.test.tester.AdminHttpClient;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(KafkaClusterExtension.class)
class MetricsIT {

    @BeforeEach
    public void beforeEach() {
        Assertions.assertThat(Metrics.globalRegistry.getMeters()).isEmpty();
    }

    @AfterEach
    public void afterEach() throws Exception {
        Assertions.assertThat(Metrics.globalRegistry.getMeters()).isEmpty();
    }

    @Test
    void shouldOfferPrometheusMetricsScrapeEndpoint(KafkaCluster cluster) {
        var config = proxy(cluster)
                .withNewAdminHttp()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endAdminHttp();

        try (var tester = kroxyliciousTester(config)) {
            String counter_name = "test_metric_" + Math.abs(new Random().nextLong()) + "_total";
            Metrics.counter(counter_name).increment();
            HttpResponse<String> response = AdminHttpClient.INSTANCE.getFromAdminEndpoint("metrics");
            assertResponseBodyContainsMeter(response, counter_name, "1.0");
            HttpResponse<String> notFoundResp = AdminHttpClient.INSTANCE.getFromAdminEndpoint("nonexistant");
            assertEquals(notFoundResp.statusCode(), HttpResponseStatus.NOT_FOUND.code());
        }
    }

    @Test
    void shouldOfferPrometheusMetricsWithNamedBinder(KafkaCluster cluster) {
        var config = proxy(cluster)
                .addToMicrometer(
                        new MicrometerDefinitionBuilder(StandardBindersHook.class.getName()).withConfig("binderNames", List.of("JvmGcMetrics")).build())
                .withNewAdminHttp()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endAdminHttp();

        try (var tester = kroxyliciousTester(config)) {
            HttpResponse<String> response = AdminHttpClient.INSTANCE.getFromAdminEndpoint("metrics");
            assertResponseBodyContainsMeter(response, "jvm_gc_memory_allocated_bytes_total");
        }
    }

    @Test
    void shouldOfferPrometheusMetricsWithCommonTags(KafkaCluster cluster) {
        var config = proxy(cluster)
                .addToMicrometer(new MicrometerDefinitionBuilder(CommonTagsHook.class.getName()).withConfig("commonTags", Map.of("a", "b")).build())
                .withNewAdminHttp()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endAdminHttp();

        try (var tester = kroxyliciousTester(config)) {
            String counter_name = "test_metric_" + Math.abs(new Random().nextLong()) + "_total";
            Metrics.counter(counter_name).increment();
            HttpResponse<String> response = AdminHttpClient.INSTANCE.getFromAdminEndpoint("metrics");
            assertResponseBodyContainsMeterWithTag(response, counter_name, "a", "b");
        }
    }

    private static void assertResponseBodyContainsMeter(HttpResponse<String> response, String meterName, String meterValue) {
        assertResponseBodyContainsMeter(response, meterName, "\\{*.*\\}*", Pattern.quote(meterValue));
    }

    private static void assertResponseBodyContainsMeter(HttpResponse<String> response, String meterName) {
        assertResponseBodyContainsMeter(response, meterName, "\\{*.*\\}*", "[0-9\\.E]+");
    }

    private static void assertResponseBodyContainsMeterWithTag(HttpResponse<String> response, String meterName, String tagKey, String tagValue) {
        assertResponseBodyContainsMeter(response, meterName, "\\{" + Pattern.quote(tagKey) + "=\"" + Pattern.quote(tagValue) + "\",\\}", "[0-9\\.E]+");
    }

    private static void assertResponseBodyContainsMeter(HttpResponse<String> response, String meterName, String labelRegex, String valueRegex) {
        assertTrue(Arrays.stream(response.body().split("\n")).anyMatch(it -> it.matches(meterName + labelRegex + " " + valueRegex)));
    }

}
