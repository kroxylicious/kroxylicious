/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.OperatorException;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.MeterNotFoundException;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaClusterRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.slf4j.LoggerFactory.getLogger;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class OperatorMainIT {
    private OperatorMain operatorMain;
    // This is an IT because it depends on having a running Kube cluster

    private final Logger log = getLogger(OperatorMainIT.class);

    @BeforeAll
    static void beforeAll() {
        LocallyRunOperatorExtension.applyCrd(KafkaProtocolFilter.class, OperatorTestUtils.kubeClientIfAvailable());
        LocallyRunOperatorExtension.applyCrd(KafkaProxy.class, OperatorTestUtils.kubeClientIfAvailable());
        LocallyRunOperatorExtension.applyCrd(VirtualKafkaCluster.class, OperatorTestUtils.kubeClientIfAvailable());
        LocallyRunOperatorExtension.applyCrd(KafkaClusterRef.class, OperatorTestUtils.kubeClientIfAvailable());
    }

    @AfterAll
    static void afterAll() {
        try (KubernetesClient kubernetesClient = OperatorTestUtils.kubeClientIfAvailable()) {
            if (kubernetesClient != null) {
                kubernetesClient.resources(KafkaProtocolFilter.class).delete();
                kubernetesClient.resources(KafkaProxy.class).delete();
                kubernetesClient.resources(VirtualKafkaCluster.class).delete();
                kubernetesClient.resources(KafkaClusterRef.class).delete();
            }
        }
    }

    @BeforeEach
    void beforeEach() {
        assertThat(Metrics.globalRegistry.getMeters()).isEmpty();
        operatorMain = new OperatorMain();
    }

    @AfterEach
    void afterEach() {
        if (operatorMain != null) {
            operatorMain.stop();
        }
        assertThat(Metrics.globalRegistry.getMeters()).isEmpty();
    }

    @Test
    void start() {
        try {
            operatorMain.start();
        }
        catch (OperatorException e) {
            fail("Exception occurred starting operator: " + e.getMessage());
        }

    }

    @Test
    void shouldRegisterPrometheusMetricsInGlobalRegistry() {
        // Given

        // When
        operatorMain.start();

        // Then
        assertThat(Metrics.globalRegistry.getRegistries())
                .isNotEmpty()
                .contains(operatorMain.getRegistry());
    }

    @Test
    void shouldRegisterOperatorMetrics() {
        // Given
        final KafkaProxyBuilder proxyBuilder = new KafkaProxyBuilder().withKind("KafkaProxy").withNewMetadata().withName("mycoolproxy").endMetadata();
        operatorMain.start();

        // When
        OperatorTestUtils.kubeClientIfAvailable().resource(proxyBuilder.build()).create();

        // Then
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .ignoreException(MeterNotFoundException.class)
                .untilAsserted(() -> {
                    log.info("looking for `\"operator.sdk.events.received\"` in meters: {}",
                            operatorMain.getRegistry()
                                    .getMeters()
                                    .stream()
                                    .map(meter -> meter.getId().getName())
                                    .distinct()
                                    .collect(Collectors.joining(", ", "[", "]")));
                    assertThat(operatorMain.getRegistry().get("operator.sdk.events.received").meter().getId()).isNotNull();
                });
    }

    @Test
    void shouldRegisterMetricsForProxyReconciler() {
        // Given

        // When
        operatorMain.start();

        // Then
        assertThat(operatorMain.getRegistry().get("operator.sdk.reconciliations.executions.proxyreconciler").meter().getId()).isNotNull();
    }
}
