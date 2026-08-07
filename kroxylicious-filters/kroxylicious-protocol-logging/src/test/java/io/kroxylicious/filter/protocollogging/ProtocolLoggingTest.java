/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.protocollogging;

import java.util.List;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.event.Level;

import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtocolLoggingTest {

    private final ProtocolLogging factory = new ProtocolLogging();

    @ParameterizedTest
    @ValueSource(ints = { 0, -1, -100, Integer.MIN_VALUE })
    void nonPositiveMaxBodyCharsIsRejected(int maxBodyChars) {
        // Given
        ProtocolLogging.Config config = new ProtocolLogging.Config(List.of(), maxBodyChars, null);

        // When / Then
        assertThatThrownBy(() -> factory.initialize(null, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("maxBodyChars must be greater than zero");
    }

    @Test
    void nullConfigProducesDefaults() {
        // When
        ProtocolLogging.Config normalized = factory.initialize(null, null);

        // Then
        assertThat(normalized.apiKeyNames()).isEmpty();
        assertThat(normalized.maxBodyChars()).isEqualTo(ProtocolLogging.DEFAULT_MAX_BODY_CHARS);
        assertThat(normalized.logLevel()).isEqualTo(ProtocolLogging.DEFAULT_LOG_LEVEL);
    }

    @Test
    void partialConfigFillsDefaults() {
        // Given
        ProtocolLogging.Config config = new ProtocolLogging.Config(List.of("METADATA"), null, null);

        // When
        ProtocolLogging.Config normalized = factory.initialize(null, config);

        // Then
        assertThat(normalized.apiKeyNames()).containsExactly("METADATA");
        assertThat(normalized.maxBodyChars()).isEqualTo(ProtocolLogging.DEFAULT_MAX_BODY_CHARS);
        assertThat(normalized.logLevel()).isEqualTo(ProtocolLogging.DEFAULT_LOG_LEVEL);
    }

    @Test
    void createFilterWithEmptyApiKeyNamesHandlesAllKeys() {
        // Given
        ProtocolLogging.Config config = new ProtocolLogging.Config(List.of(), 8192, Level.DEBUG);

        // When
        RequestFilter filter = (RequestFilter) factory.createFilter(null, config);

        // Then
        assertThat(filter.shouldHandleRequest(ApiKeys.METADATA, (short) 12)).isTrue();
        assertThat(filter.shouldHandleRequest(ApiKeys.PRODUCE, (short) 9)).isTrue();
    }

    @Test
    void createFilterWithPopulatedApiKeyNamesHandlesOnlyThoseKeys() {
        // Given
        ProtocolLogging.Config config = new ProtocolLogging.Config(List.of("METADATA"), 8192, Level.DEBUG);

        // When
        RequestFilter filter = (RequestFilter) factory.createFilter(null, config);

        // Then
        assertThat(filter.shouldHandleRequest(ApiKeys.METADATA, (short) 12)).isTrue();
        assertThat(filter.shouldHandleRequest(ApiKeys.PRODUCE, (short) 9)).isFalse();
    }

}
