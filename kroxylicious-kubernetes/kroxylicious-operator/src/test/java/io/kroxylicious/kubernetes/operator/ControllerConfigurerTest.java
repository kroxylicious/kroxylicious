/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearEnvironmentVariable;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import static io.kroxylicious.kubernetes.operator.OperatorMain.KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME;
import static io.kroxylicious.kubernetes.operator.OperatorMain.KROXYLICIOUS_WATCHED_NAMESPACES_VAR_NAME;
import static org.assertj.core.api.Assertions.assertThat;

class ControllerConfigurerTest {

    @Test
    @ClearEnvironmentVariable(key = KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME)
    @ClearEnvironmentVariable(key = KROXYLICIOUS_WATCHED_NAMESPACES_VAR_NAME)
    void shouldUseDefaultsWhenNotConfigured() {
        // Given

        // When
        ControllerConfigurer configurer = new ControllerConfigurer();

        // Then
        assertThat(configurer.getWatchedNamespaces()).isNull();
    }

    @Test
    @SetEnvironmentVariable(key = KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME, value = "300")
    @ClearEnvironmentVariable(key = KROXYLICIOUS_WATCHED_NAMESPACES_VAR_NAME)
    void shouldUseConfiguredMaxReconciliationInterval() {
        // Given

        // When
        ControllerConfigurer configurer = new ControllerConfigurer();

        // Then
        assertThat(configurer.getWatchedNamespaces()).isNull();
    }

    @Test
    @SetEnvironmentVariable(key = KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME, value = "invalid")
    @ClearEnvironmentVariable(key = KROXYLICIOUS_WATCHED_NAMESPACES_VAR_NAME)
    void shouldUseDefaultMaxReconciliationIntervalWhenInvalidValue() {
        // Given

        // When
        ControllerConfigurer configurer = new ControllerConfigurer();

        // Then
        assertThat(configurer.getWatchedNamespaces()).isNull();
    }

    @Test
    @SetEnvironmentVariable(key = KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME, value = "0")
    @ClearEnvironmentVariable(key = KROXYLICIOUS_WATCHED_NAMESPACES_VAR_NAME)
    void shouldUseDefaultMaxReconciliationIntervalWhenZero() {
        // Given

        // When
        ControllerConfigurer configurer = new ControllerConfigurer();

        // Then
        assertThat(configurer.getWatchedNamespaces()).isNull();
    }

    @Test
    @SetEnvironmentVariable(key = KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME, value = "-60")
    @ClearEnvironmentVariable(key = KROXYLICIOUS_WATCHED_NAMESPACES_VAR_NAME)
    void shouldUseDefaultMaxReconciliationIntervalWhenNegative() {
        // Given

        // When
        ControllerConfigurer configurer = new ControllerConfigurer();

        // Then
        assertThat(configurer.getWatchedNamespaces()).isNull();
    }

    @Test
    @ClearEnvironmentVariable(key = KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME)
    @ClearEnvironmentVariable(key = KROXYLICIOUS_WATCHED_NAMESPACES_VAR_NAME)
    void shouldWatchAllNamespacesWhenNotConfigured() {
        // Given

        // When
        ControllerConfigurer configurer = new ControllerConfigurer();

        // Then
        assertThat(configurer.getWatchedNamespaces()).isNull();
    }

    @Test
    @ClearEnvironmentVariable(key = KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME)
    @SetEnvironmentVariable(key = KROXYLICIOUS_WATCHED_NAMESPACES_VAR_NAME, value = "")
    void shouldWatchAllNamespacesWhenEmpty() {
        // Given

        // When
        ControllerConfigurer configurer = new ControllerConfigurer();

        // Then
        assertThat(configurer.getWatchedNamespaces()).isNull();
    }

    @Test
    @ClearEnvironmentVariable(key = KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME)
    @SetEnvironmentVariable(key = KROXYLICIOUS_WATCHED_NAMESPACES_VAR_NAME, value = "single")
    void shouldWatchSingleNamespace() {
        // Given

        // When
        ControllerConfigurer configurer = new ControllerConfigurer();

        // Then
        assertThat(configurer.getWatchedNamespaces()).containsExactly("single");
    }

    @Test
    @ClearEnvironmentVariable(key = KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME)
    @SetEnvironmentVariable(key = KROXYLICIOUS_WATCHED_NAMESPACES_VAR_NAME, value = "abc,def,ghi")
    void shouldWatchMultipleNamespaces() {
        // Given

        // When
        ControllerConfigurer configurer = new ControllerConfigurer();

        // Then
        assertThat(configurer.getWatchedNamespaces()).containsExactlyInAnyOrder("abc", "def", "ghi");
    }

    @Test
    @ClearEnvironmentVariable(key = KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME)
    @SetEnvironmentVariable(key = KROXYLICIOUS_WATCHED_NAMESPACES_VAR_NAME, value = " abc ,def ")
    void shouldTrimNamespaceNames() {
        // Given

        // When
        ControllerConfigurer configurer = new ControllerConfigurer();

        // Then
        assertThat(configurer.getWatchedNamespaces()).containsExactlyInAnyOrder("abc", "def");
    }

    @Test
    @ClearEnvironmentVariable(key = KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS_VAR_NAME)
    @SetEnvironmentVariable(key = KROXYLICIOUS_WATCHED_NAMESPACES_VAR_NAME, value = ",abc,")
    void shouldIgnoreEmptyNamespaces() {
        // Given

        // When
        ControllerConfigurer configurer = new ControllerConfigurer();

        // Then
        assertThat(configurer.getWatchedNamespaces()).containsExactly("abc");
    }
}
