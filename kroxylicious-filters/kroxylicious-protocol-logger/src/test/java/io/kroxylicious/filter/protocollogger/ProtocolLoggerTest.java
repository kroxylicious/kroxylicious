/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.protocollogger;

import java.util.Set;
import java.util.stream.Stream;

import io.kroxylicious.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.event.Level;

import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class ProtocolLoggerTest {

    private final ProtocolLogger factory = new ProtocolLogger();

    @Test
    void nullConfigProducesDefaults() {
        // When
        ProtocolLogger.Config normalized = factory.initialize(null, null);

        // Then
        assertThat(normalized.apiKeyNames()).isEmpty();
        assertThat(normalized.logLevel()).isEqualTo(ProtocolLogger.DEFAULT_LOG_LEVEL);
        assertThat(normalized.loggerName()).isEqualTo(ProtocolLoggerFilter.class.getName());
    }

    @Test
    void partialConfigFillsDefaults() {
        // Given
        ProtocolLogger.Config config = new ProtocolLogger.Config(Set.of("METADATA"), null, null);

        // When
        ProtocolLogger.Config normalized = factory.initialize(null, config);

        // Then
        assertThat(normalized.apiKeyNames()).containsExactly("METADATA");
        assertThat(normalized.logLevel()).isEqualTo(ProtocolLogger.DEFAULT_LOG_LEVEL);
        assertThat(normalized.loggerName()).isEqualTo(ProtocolLoggerFilter.class.getName());
    }

    @Test
    void createFilterWithEmptyApiKeyNamesHandlesAllKeys() {
        // Given
        ProtocolLogger.Config config = new ProtocolLogger.Config(Set.of(), Level.DEBUG, ProtocolLoggerFilter.class.getName());

        // When
        RequestFilter filter = (RequestFilter) factory.createFilter(null, config);

        // Then
        assertThat(filter.shouldHandleRequest(ApiKeys.METADATA, (short) 12)).isTrue();
        assertThat(filter.shouldHandleRequest(ApiKeys.PRODUCE, (short) 9)).isTrue();
    }

    @Test
    void createFilterWithPopulatedApiKeyNamesHandlesOnlyThoseKeys() {
        // Given
        ProtocolLogger.Config config = new ProtocolLogger.Config(Set.of("METADATA"), Level.DEBUG, ProtocolLoggerFilter.class.getName());

        // When
        RequestFilter filter = (RequestFilter) factory.createFilter(null, config);

        // Then
        assertThat(filter.shouldHandleRequest(ApiKeys.METADATA, (short) 12)).isTrue();
        assertThat(filter.shouldHandleRequest(ApiKeys.PRODUCE, (short) 9)).isFalse();
    }

    @Test
    void defaultLoggerNameIsFilterClassName() {
        // When
        ProtocolLogger.Config normalized = factory.initialize(null, null);

        // Then
        assertThat(normalized.loggerName()).isEqualTo(ProtocolLoggerFilter.class.getName());
    }

    @Test
    void configuredLoggerNameIsUsed() {
        // Given
        ProtocolLogger.Config config = new ProtocolLogger.Config(null, null, "protocol.downstream");

        // When
        ProtocolLogger.Config normalized = factory.initialize(null, config);

        // Then
        assertThat(normalized.loggerName()).isEqualTo("protocol.downstream");
    }

    @Test
    void twoInstancesWithDifferentLoggerNamesEmitUnderDifferentLoggers() {
        // Given
        ProtocolLogger.Config configA = new ProtocolLogger.Config(Set.of(), Level.DEBUG, "protocol.downstream");
        ProtocolLogger.Config configB = new ProtocolLogger.Config(Set.of(), Level.DEBUG, "protocol.upstream");

        // When
        ProtocolLogger.Config normalizedA = factory.initialize(null, configA);
        ProtocolLogger.Config normalizedB = factory.initialize(null, configB);

        // Then
        assertThat(normalizedA.loggerName()).isNotEqualTo(normalizedB.loggerName());
    }

    static Stream<Arguments> apiKeyNameResolution() {
        return Stream.of(
                argumentSet("canonical name resolves", "METADATA", "METADATA"),
                argumentSet("docs-style FindCoordinator resolves", "FindCoordinator", "FIND_COORDINATOR"),
                argumentSet("lowercase resolves", "metadata", "METADATA"),
                argumentSet("hyphenated resolves", "find-coordinator", "FIND_COORDINATOR"));
    }

    @ParameterizedTest
    @MethodSource("apiKeyNameResolution")
    void apiKeyNameIsResolvedForgivingly(String input, String expectedResolved) {
        // Given
        ProtocolLogger.Config config = new ProtocolLogger.Config(Set.of(input), null, null);

        // When
        ProtocolLogger.Config normalized = factory.initialize(null, config);

        // Then
        assertThat(normalized.apiKeyNames()).containsExactly(expectedResolved);
    }

    @Test
    void duplicateApiKeyNamesCollapse() {
        // Given
        ProtocolLogger.Config config = new ProtocolLogger.Config(Set.of("METADATA", "Metadata"), null, null);

        // When
        ProtocolLogger.Config normalized = factory.initialize(null, config);

        // Then
        assertThat(normalized.apiKeyNames()).containsExactly("METADATA");
    }

    @Test
    void unresolvableApiKeyNameThrowsWithBadValue() {
        // Given
        ProtocolLogger.Config config = new ProtocolLogger.Config(Set.of("NOT_A_REAL_API_KEY"), null, null);

        // When / Then
        assertThatThrownBy(() -> factory.initialize(null, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("NOT_A_REAL_API_KEY");
    }

}
