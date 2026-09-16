/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.proxy.filter.FilterContext;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class MdsBrokerVersionsTest {
    private final FilterContext context = mock(FilterContext.class);
    private final MdsBrokerVersions versions = new MdsBrokerVersions();

    @ParameterizedTest
    @CsvSource({ "17,0,0", "36,0,0", "17,2,3", "36,2,3" })
    void rejectsBrokersWhoseRangeExcludesVersionOne(short apiKey, short minimum, short maximum) {
        // Given
        var reply = TestSaslVersions.supported();
        reply.apiKeys().find(apiKey).setMinVersion(minimum).setMaxVersion(maximum);
        doReturn(CompletableFuture.completedStage(reply)).when(context).sendRequest(any(), any());

        // When
        // Then
        assertThatThrownBy(() -> versions.check(context).toCompletableFuture().join())
                .hasCauseInstanceOf(MdsFailure.class).hasMessageContaining("UPSTREAM_VERSIONS");
    }

    @ParameterizedTest
    @ValueSource(ints = { 17, 36 })
    void rejectsMissingSaslApis(int apiKey) {
        // Given
        var reply = TestSaslVersions.supported();
        reply.apiKeys().removeIf(api -> api.apiKey() == apiKey);
        doReturn(CompletableFuture.completedStage(reply)).when(context).sendRequest(any(), any());

        // When
        // Then
        assertThatThrownBy(() -> versions.check(context).toCompletableFuture().join())
                .hasCauseInstanceOf(MdsFailure.class).hasMessageContaining("UPSTREAM_VERSIONS");
    }

    @Test
    void reusesNegotiationOnTheSameConnection() {
        // Given
        doReturn(CompletableFuture.completedStage(TestSaslVersions.supported())).when(context).sendRequest(any(), any());

        // When
        versions.check(context).toCompletableFuture().join();
        versions.check(context).toCompletableFuture().join();

        // Then
        verify(context, times(1)).sendRequest(any(), any());
    }
}
