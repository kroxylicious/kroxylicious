/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import io.javaoperatorsdk.operator.OperatorException;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class OperatorMainIT {
    // This is an IT because it depends on having a running Kube cluster

    @Test
    void run() {
        try {
            OperatorMain.run();
        }
        catch (OperatorException e) {
            fail("Exception occurred starting operator: " + e.getMessage());
        }

    }

    @Test
    void shouldRegisterPrometheusMetrics() {
        // Given

        // When
        OperatorMain.run();

        // Then
        assertThat(Metrics.globalRegistry.getRegistries())
                .isNotEmpty()
                .hasAtLeastOneElementOfType(PrometheusMeterRegistry.class)
                .element(0)
                .satisfies(meterRegistry ->
                        assertThat(meterRegistry.get("operator.sdk.reconciliations.success"))
                                .isNotNull());
    }

    @Test
    void shouldRegisterOperatorMetrics() {
        // Given

        // When
        OperatorMain.run();

        // Then
        assertThat(Metrics.globalRegistry.get("operator.sdk.reconciliations.success"))
                .isNotNull();
    }

    @Test
    void shouldRegisterMetricsForProxyReconciler() {
        // Given

        // When
        OperatorMain.run();

        // Then
        assertThat(Metrics.globalRegistry.get("operator.sdk.reconciliations.proxyreconciler.success")).isNotNull();
    }
}