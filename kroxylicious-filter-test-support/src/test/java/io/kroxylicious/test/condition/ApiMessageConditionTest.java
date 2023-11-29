/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.condition;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.requests.ProduceRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
}