/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.audit.emitter.metrics;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.audit.AuditEmitter;

import static org.assertj.core.api.Assertions.assertThat;

class MetricsEmitterFactoryTest {
    @Test
    void shouldCreateMetricsEmitterWithoutMapping() {
        // Given
        MetricsEmitterConfig configuration = new MetricsEmitterConfig(null);
        // When
        AuditEmitter actual = new MetricsEmitterFactory().create(configuration);
        // Then
        assertThat(actual).isNotNull();
    }

    @Test
    void shouldCreateMetricsEmitterWithMapping() {
        // Given
        MetricsEmitterConfig configuration = new MetricsEmitterConfig(Map.of("vc", "virtual_cluster"));
        // When
        AuditEmitter actual = new MetricsEmitterFactory().create(configuration);
        // Then
        assertThat(actual).isNotNull();
    }

}