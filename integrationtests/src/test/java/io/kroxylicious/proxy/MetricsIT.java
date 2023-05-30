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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.micrometer.core.instrument.Metrics;
import io.netty.handler.codec.http.HttpResponseStatus;

import io.kroxylicious.test.tester.AdminHttpClient;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(KafkaClusterExtension.class)
public class MetricsIT {

    @BeforeEach
    public void beforeEach() {
        Metrics.globalRegistry.clear();
    }

    @Test
    public void shouldOfferPrometheusMetricsScrapeEndpoint(KafkaCluster cluster) {
        KroxyliciousConfigBuilder config = proxy(cluster)
                .withNewAdminHttp()
                .withNewEndpoints()
                .withPrometheusEndpointConfig(Map.of())
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
    public void shouldOfferPrometheusMetricsWithNamedBinder(KafkaCluster cluster) {
        KroxyliciousConfigBuilder config = proxy(cluster)
                .addToMicrometer(new MicrometerConfigBuilder()
                        .withType("StandardBinders")
                        .withConfig(Map.of("binderNames", List.of("JvmGcMetrics")))
                        .build())
                .withNewAdminHttp()
                .withNewEndpoints()
                .withPrometheusEndpointConfig(Map.of())
                .endEndpoints()
                .endAdminHttp();

        try (var tester = kroxyliciousTester(config)) {
            HttpResponse<String> response = AdminHttpClient.INSTANCE.getFromAdminEndpoint("metrics");
            assertResponseBodyContainsMeter(response, "jvm_gc_memory_allocated_bytes_total");
        }
    }

    @Test
    public void shouldOfferPrometheusMetricsWithCommonTags(KafkaCluster cluster) {
        KroxyliciousConfigBuilder config = proxy(cluster)
                .addNewMicrometer().withType("CommonTags").withConfig(Map.of("commonTags", Map.of("a", "b"))).endMicrometer()
                .withNewAdminHttp()
                .withNewEndpoints()
                .withPrometheusEndpointConfig(Map.of())
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
