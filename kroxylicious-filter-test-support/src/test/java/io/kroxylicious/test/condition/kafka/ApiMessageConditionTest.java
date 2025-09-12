/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.condition.kafka;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.assertj.core.description.Description;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

class ApiMessageConditionTest {

    private ProduceRequestData produceRequestData;

    @BeforeEach
    void setUp() {
        produceRequestData = new ProduceRequestData();
    }

    @Test
    void shouldReturnFalseIfActualIsNull() {
        // Given
        ApiMessageCondition<ProduceRequestData> apiMessageCondition = new ApiMessageCondition<>(o -> true);

        // When
        final boolean matches = apiMessageCondition.matches(null);

        // Then
        assertThat(matches).isFalse();
    }

    @Test
    void shouldReturnPredicateResultTrue() {
        // Given
        ApiMessageCondition<ProduceRequestData> apiMessageCondition = new ApiMessageCondition<>(o -> true);

        // When
        final boolean matches = apiMessageCondition.matches(produceRequestData);

        // Then
        assertThat(matches).isTrue();
    }

    @Test
    void shouldReturnPredicateResultFalse() {
        // Given
        ApiMessageCondition<ProduceRequestData> apiMessageCondition = new ApiMessageCondition<>(o -> false);

        // When
        final boolean matches = apiMessageCondition.matches(produceRequestData);

        // Then
        assertThat(matches).isFalse();
    }

    @Test
    void shouldSupportCustomDescriptions() {
        // Given
        ApiMessageCondition<ProduceRequestData> apiMessageCondition = new ApiMessageCondition<>(o -> false);

        // When
        final Description description = apiMessageCondition.description();

        // Then
        assertThat(description).extracting(Description::value).asString().isNotBlank().isEqualTo("an Api Message matching a custom predicate");
    }

    @ParameterizedTest
    @EnumSource(ApiKeys.class)
    void shouldCreateConditionForApiKeyEnum(ApiKeys apiKey) {
        // Given
        ApiMessageCondition<ApiMessage> apiMessageCondition = ApiMessageCondition.forApiKey(apiKey);

        // When
        final boolean matches = apiMessageCondition.matches(produceRequestData);

        // Then
        assertThat(matches).describedAs("expected %s (%s) to return %s for ProduceRequestData", apiKey, ApiKeys.PRODUCE == apiKey).isEqualTo(ApiKeys.PRODUCE == apiKey);
    }

    @ParameterizedTest
    @EnumSource(ApiKeys.class)
    void shouldCreateConditionForApiKeyShort(ApiKeys apiKey) {
        // Given
        ApiMessageCondition<ApiMessage> apiMessageCondition = ApiMessageCondition.forApiKey(apiKey.id);

        // When
        final boolean matches = apiMessageCondition.matches(produceRequestData);

        // Then
        assertThat(matches).describedAs("expected %s (%s) to return %s for ProduceRequestData", apiKey, apiKey.id, ApiKeys.PRODUCE == apiKey)
                .isEqualTo(ApiKeys.PRODUCE == apiKey);
    }

    @ParameterizedTest
    @EnumSource(ApiKeys.class)
    void shouldCreateCustomDescriptionForApiKeyEnum(ApiKeys apiKey) {
        // Given
        ApiMessageCondition<ApiMessage> apiMessageCondition = ApiMessageCondition.forApiKey(apiKey);

        // When
        final Description description = apiMessageCondition.description();

        // Then
        assertThat(description).extracting(Description::value)
                .asString()
                .isNotBlank()
                .contains(apiKey.name())
                .contains(String.valueOf(apiKey.id));
    }

    @ParameterizedTest
    @EnumSource(ApiKeys.class)
    void shouldCreateCustomDescriptionForApiKeyShort(ApiKeys apiKey) {
        // Given
        ApiMessageCondition<ApiMessage> apiMessageCondition = ApiMessageCondition.forApiKey(apiKey.id);

        // When
        final Description description = apiMessageCondition.description();

        // Then
        assertThat(description).extracting(Description::value)
                .asString()
                .isNotBlank()
                .contains(apiKey.name())
                .contains(String.valueOf(apiKey.id));
    }

}
