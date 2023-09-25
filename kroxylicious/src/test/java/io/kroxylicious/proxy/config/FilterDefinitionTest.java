/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.internal.filter.ExampleConfig;

import static io.kroxylicious.proxy.internal.filter.TestFilterContributor.OPTIONAL_CONFIG_FILTER;
import static io.kroxylicious.proxy.internal.filter.TestFilterContributor.REQUIRED_CONFIG_FILTER;
import static org.assertj.core.api.Assertions.assertThat;

class FilterDefinitionTest {

    @Test
    void shouldFailValidationIfRequireConfigMissing() {
        // Given
        final FilterDefinition requiredConfig = new FilterDefinition(REQUIRED_CONFIG_FILTER, null);

        // When
        final boolean actual = requiredConfig.isDefinitionValid();

        // Then
        assertThat(actual).isFalse();
    }

    @Test
    void shouldPassValidationIfRequireConfigSupplied() {
        // Given
        final FilterDefinition requiredConfig = new FilterDefinition(REQUIRED_CONFIG_FILTER, new ExampleConfig());

        // When
        final boolean actual = requiredConfig.isDefinitionValid();

        // Then
        assertThat(actual).isTrue();
    }

    @Test
    void shouldPassValidationIfOptionalConfigSupplied() {
        // Given
        final FilterDefinition requiredConfig = new FilterDefinition(OPTIONAL_CONFIG_FILTER, new ExampleConfig());

        // When
        final boolean actual = requiredConfig.isDefinitionValid();

        // Then
        assertThat(actual).isTrue();
    }

    @Test
    void shouldPassValidationIfOptionalConfigIsMissing() {
        // Given
        final FilterDefinition requiredConfig = new FilterDefinition(OPTIONAL_CONFIG_FILTER, null);

        // When
        final boolean actual = requiredConfig.isDefinitionValid();

        // Then
        assertThat(actual).isTrue();
    }
}