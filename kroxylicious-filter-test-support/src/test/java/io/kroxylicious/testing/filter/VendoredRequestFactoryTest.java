/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.filter;

import java.util.EnumSet;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.testing.filter.condition.kafka.VendoredApiMessageCondition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VendoredRequestFactoryTest {

    @Test
    void shouldGenerateRequestForEveryApiKey() {
        // Given
        var expectedKeys = VendoredRequestFactory.supportedApiKeys();
        var absentKeys = EnumSet.complementOf(EnumSet.copyOf(expectedKeys));

        // When
        final Stream<VendoredRequestFactory.ApiMessageVersion> apiMessages = VendoredRequestFactory.apiMessageFor(ApiKeys::latestVersion);

        // Then
        final Map<ApiKeys, ApiMessage> allMessages = apiMessages.collect(
                Collectors.toMap(
                        apiMessageVersion -> ApiKeys.forId(apiMessageVersion.apiMessage().apiKey()),
                        VendoredRequestFactory.ApiMessageVersion::apiMessage));
        assertThat(allMessages).hasSameSizeAs(expectedKeys)
                .allSatisfy((apiKeys, apiMessage) -> {
                    assertThat(apiKeys).isNotIn(absentKeys);
                    assertThat(apiMessages).isNotNull();
                });
    }

    @Test
    void shouldCreateProduceMessage() {
        // Given

        // When
        final Stream<VendoredRequestFactory.ApiMessageVersion> apiMessages = VendoredRequestFactory.apiMessageFor(ApiKeys::latestVersion, ApiKeys.PRODUCE);

        // Then
        final Map<ApiKeys, ApiMessage> allMessages = apiMessages.collect(
                Collectors.toMap(
                        apiMessageVersion -> ApiKeys.forId(apiMessageVersion.apiMessage().apiKey()),
                        VendoredRequestFactory.ApiMessageVersion::apiMessage));
        assertThat(allMessages).hasSize(1).hasValueSatisfying(VendoredApiMessageCondition.forApiKey(ApiKeys.PRODUCE));
    }

    @ParameterizedTest
    @MethodSource("specialCases")
    void shouldThrowExceptionIfSpecialCaseRequested(ApiKeys apiKey) {
        // Given

        // When
        assertThatThrownBy(() -> VendoredRequestFactory.apiMessageFor(ApiKeys::latestVersion, apiKey)).isInstanceOf(IllegalArgumentException.class);

        // Then
    }

    static Stream<ApiKeys> specialCases() {
        return VendoredRequestFactory.SPECIAL_CASES.stream();
    }

    @ParameterizedTest
    @MethodSource("removedApiKeys")
    void shouldThrowExceptionIfRemovedCaseRequested(ApiKeys apiKey) {
        // Given

        // When
        assertThatThrownBy(() -> VendoredRequestFactory.apiMessageFor(ApiKeys::latestVersion, apiKey)).isInstanceOf(IllegalArgumentException.class);

        // Then
    }

    static Stream<ApiKeys> removedApiKeys() {
        return VendoredRequestFactory.REMOVED_API_KEYS.stream();
    }
}