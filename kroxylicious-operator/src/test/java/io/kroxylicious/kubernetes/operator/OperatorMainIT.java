/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.javaoperatorsdk.operator.OperatorException;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;
import io.micrometer.core.instrument.Metrics;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class OperatorMainIT {
    private OperatorMain operatorMain;
    // This is an IT because it depends on having a running Kube cluster

    @RegisterExtension
    static final LocallyRunOperatorExtension operatorExtension = LocallyRunOperatorExtension.builder()
            .withAdditionalCustomResourceDefinition(KafkaProtocolFilter.class)
            .withAdditionalCustomResourceDefinition(KafkaProxy.class)
            .withKubernetesClient(OperatorTestUtils.kubeClientIfAvailable())
            .build();

    @BeforeEach
    void beforeEach() {
        assertThat(Metrics.globalRegistry.getMeters()).isEmpty();
        operatorMain = new OperatorMain(() -> Metrics.globalRegistry);
    }

    @AfterEach
    void afterEach() {
        if (operatorMain != null) {
            operatorMain.stop();
        }
        assertThat(Metrics.globalRegistry.getMeters()).isEmpty();
    }

    @Test
    void run() {
        try {
            operatorMain.run();
        }
        catch (OperatorException e) {
            fail("Exception occurred starting operator: " + e.getMessage());
        }

    }

    @Test
    void shouldRegisterPrometheusMetricsInGlobalRegistry() {
        // Given

        // When
        operatorMain.run();

        // Then
        assertThat(Metrics.globalRegistry.getRegistries())
                .isNotEmpty()
                .contains(operatorMain.getRegistry());
    }

    @Test
    void shouldRegisterOperatorMetrics() {
        // Given

        // When
        operatorMain.run();

        // Then
        assertThat(operatorMain.getRegistry().get("operator.sdk.events.received").meter().getId()).isNotNull();
    }

    @Test
    void shouldRegisterMetricsForProxyReconciler() {
        // Given

        // When
        operatorMain.run();

        // Then
        assertThat(operatorMain.getRegistry().get("operator.sdk.reconciliations.executions.proxyreconciler").meter().getId()).isNotNull();
    }
}
