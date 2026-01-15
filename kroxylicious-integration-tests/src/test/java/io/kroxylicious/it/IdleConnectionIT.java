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
import java.util.function.Predicate;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NetworkDefinition;
import io.kroxylicious.proxy.config.NetworkDefinitionBuilder;
import io.kroxylicious.test.tester.ManagementClient;
import io.kroxylicious.test.tester.SimpleMetric;
import io.kroxylicious.test.tester.SimpleMetricAssert;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
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

            var initialConnectionCound = getActiveConnectionCount(managementClient.scrapeMetrics(),
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

    @Test
    void shouldTimeOutAuthenticatedCLient(@SaslMechanism(value = "PLAIN", principals = {
                                     @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster) {

        String mechanism = "PLAIN";
        String clientLoginModule = "org.apache.kafka.common.security.plain.PlainLoginModule";
        String username = "alice";
        String password = "alice-secret";

        ConfigurationBuilder configBuilder = buildProxyConfig(cluster, Set.of(mechanism), null);
        configBuilder.withNetwork(new NetworkDefinitionBuilder().withNewProxy().withAuthenticatedIdleTimeout(Duration.ofSeconds(2)).endProxy().build());
        configBuilder.withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();

        String jassConfig = "%s required%n  username=\"%s\"%n   password=\"%s\";".formatted(clientLoginModule, username, password);
        var producerConfig = Map.<String, Object> of(
                CommonClientConfigs.CLIENT_ID_CONFIG, mechanism + "-producer",
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                SaslConfigs.SASL_MECHANISM, mechanism,
                SaslConfigs.SASL_JAAS_CONFIG, jassConfig);

        // we need to create a producer or a consumer to get it to connect, but we don't actually use it.
        try (var tester = kroxyliciousTester(configBuilder);
                var ignored = tester.producer(producerConfig);
                ManagementClient managementClient = tester.getManagementClient()) {

            var initialConnectionCound = getActiveConnectionCount(managementClient.scrapeMetrics(),
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

    private static double getActiveConnectionCount(List<SimpleMetric> metricList, Predicate<Map<String, String>> labelPredicate) {
        return metricList.stream()
                .filter(simpleMetric -> simpleMetric.name().equals("kroxylicious_client_to_proxy_active_connections"))
                .filter(simpleMetric -> labelPredicate.test(simpleMetric.labels()))
                .findFirst()
                .orElseThrow().value();
    }

}
