/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompositeEnforcementTest {

    private static final short ONE = 1;
    private static final short TWO = 2;
    private static final short THREE = 3;
    private static final short FOUR = 4;

    private List<ApiEnforcement<ApiMessage, ApiMessage>> contiguousVersions;

    @BeforeEach
    void setUp() {
        contiguousVersions = List.of(new StubApiEnforcement(ONE, TWO), new StubApiEnforcement(THREE, FOUR));
    }

    @Test
    void shouldUseLowestMin() {
        // Given

        // When
        CompositeEnforcement<ApiMessage, ApiMessage> compositeEnforcement = new CompositeEnforcement<>(contiguousVersions);

        // Then
        assertThat(compositeEnforcement.minSupportedVersion()).isEqualTo(ONE);
    }

    @Test
    void shouldUseHighestMax() {
        // Given

        // When
        CompositeEnforcement<ApiMessage, ApiMessage> compositeEnforcement = new CompositeEnforcement<>(contiguousVersions);

        // Then
        assertThat(compositeEnforcement.maxSupportedVersion()).isEqualTo(FOUR);
    }

    @Test
    void shouldDetectOverlappingRange() {
        // Given
        List<ApiEnforcement<ApiMessage, ApiMessage>> overlappingVersions = List.of(new StubApiEnforcement(ONE, TWO), new StubApiEnforcement(TWO, FOUR));

        // When
        // Then
        assertThatThrownBy(() -> new CompositeEnforcement<>(overlappingVersions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(" is already supported");

    }

    @Test
    void shouldDetectDisjointRange() {
        // Given
        List<ApiEnforcement<ApiMessage, ApiMessage>> overlappingVersions = List.of(new StubApiEnforcement(ONE, ONE), new StubApiEnforcement(THREE, FOUR));

        // When
        // Then
        assertThatThrownBy(() -> new CompositeEnforcement<>(overlappingVersions))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Api version range support must be contiguous");
    }

    private static class StubApiEnforcement extends ApiEnforcement<ApiMessage, ApiMessage> {
        private final short minVersion;
        private final short maxVersion;

        private StubApiEnforcement(short minVersion, short maxVersion) {
            this.minVersion = minVersion;
            this.maxVersion = maxVersion;
        }

        @Override
        short minSupportedVersion() {
            return minVersion;
        }

        @Override
        short maxSupportedVersion() {
            return maxVersion;
        }

        @Override
        CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, ApiMessage request, FilterContext context, AuthorizationFilter authorizationFilter) {
            throw new UnsupportedOperationException();
        }
    }
}