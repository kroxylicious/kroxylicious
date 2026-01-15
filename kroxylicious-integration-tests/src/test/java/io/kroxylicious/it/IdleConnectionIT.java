/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.config.NetworkDefinition;
import io.kroxylicious.proxy.config.NetworkDefinitionBuilder;
import io.kroxylicious.test.tester.ManagementClient;
import io.kroxylicious.test.tester.SimpleMetric;
import io.kroxylicious.test.tester.SimpleMetricAssert;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.proxy.internal.util.Metrics.NODE_ID_LABEL;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.awaitility.Awaitility.await;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class IdleConnectionIT extends SaslInspectionIT {

    @Test
    void shouldDisconnectIdleClient(KafkaCluster cluster) {

        NetworkDefinition networkDefinition = new NetworkDefinitionBuilder().withNewProxy().withUnAuthenticatedIdleTimeout(Duration.ofSeconds(2)).endProxy().build();
        var config = proxy(cluster)
                .withNetwork(networkDefinition)
                .withNewManagement()
                    .withNewEndpoints()
                        .withNewPrometheus()
                    .endPrometheus()
                    .endEndpoints()
                .endManagement();

        // we need to create a producer or a consumer to get it to connect, but we don't actually use it.
        try (var tester = kroxyliciousTester(config);
                var ignored = tester.producer(Map.of());
                ManagementClient managementClient = tester.getManagementClient()) {

            var initialConnectionCound = getSingleMetricsValue(managementClient.scrapeMetrics(), "kroxylicious_client_to_proxy_active_connections",
                    labels -> labels.containsKey(NODE_ID_LABEL));

            await().atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> {
                        List<SimpleMetric> metricList = managementClient.scrapeMetrics();
                        SimpleMetricAssert.assertThat(metricList)
                                .withUniqueMetric("kroxylicious_client_to_proxy_active_connections", Map.of(
                                        NODE_ID_LABEL, "0"))
                                .value()
                                .isLessThan(initialConnectionCound);
                        SimpleMetricAssert.assertThat(metricList)
                                .withUniqueMetric("kroxylicious_client_to_proxy_idle_disconnects_total", Map.of(
                                        NODE_ID_LABEL, "0"))
                                .value()
                                .isGreaterThanOrEqualTo(1.0);
                    });

        }
    }

    private static double getSingleMetricsValue(List<SimpleMetric> metricList, String metricsName, Predicate<Map<String, String>> labelPredicate) {
        return metricList.stream()
                .filter(simpleMetric -> simpleMetric.name().equals(metricsName))
                .filter(simpleMetric -> labelPredicate.test(simpleMetric.labels()))
                .findFirst()
                .orElseThrow().value();
    }

}
