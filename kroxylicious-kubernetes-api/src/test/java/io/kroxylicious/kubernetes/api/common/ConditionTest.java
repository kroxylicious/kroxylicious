/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import static org.assertj.core.api.Assertions.assertThat;

class ConditionTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Test
    void serializedFormShouldUseIso8601() throws JsonProcessingException {
        // Given
        Condition build = new ConditionBuilder()
                .withLastTransitionTime(Instant.EPOCH)
                .withObservedGeneration(345678L)
                .withReason("")
                .withMessage("")
                .withType(Condition.Type.Ready)
                .build();
        // When
        var conditionWithEpoch = objectMapper.writeValueAsString(build);

        // Then
        assertThat(conditionWithEpoch).contains("\"lastTransitionTime\":\"1970-01-01T00:00:00Z\"");
    }

    @Test
    void roundTrip() throws JsonProcessingException {
        // Given
        Condition wroteCondition = new ConditionBuilder()
                .withLastTransitionTime(Instant.now())
                .withObservedGeneration(1L)
                .withReason("")
                .withMessage("")
                .withType(Condition.Type.Ready)
                .build();
        var conditionString = objectMapper.writeValueAsString(wroteCondition);

        // When
        var readCondition = objectMapper.readValue(conditionString, Condition.class);

        // Then
        assertThat(readCondition).isEqualTo(wroteCondition);
    }

    @Test
    void shouldReturnBuilder() {
        // Given
        ConditionBuilder originalBuilder = new ConditionBuilder();
        var condition = originalBuilder
                .withLastTransitionTime(Instant.EPOCH)
                .withObservedGeneration(345678L)
                .withReason("")
                .withMessage("")
                .withType(Condition.Type.Ready)
                .build();

        // When
        ConditionBuilder actualBuilder = condition.edit();

        // Then
        assertThat(actualBuilder).isNotNull().isNotSameAs(originalBuilder);
    }

}