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
    private Condition readyCondition;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        readyCondition = new ConditionBuilder()
                .withLastTransitionTime(Instant.EPOCH)
                .withObservedGeneration(345678L)
                .withReason("")
                .withMessage("")
                .withType(Condition.Type.Ready)
                .build();
    }

    @Test
    void serializedFormShouldUseIso8601() throws JsonProcessingException {
        // Given
        // When
        var conditionWithEpoch = objectMapper.writeValueAsString(readyCondition);

        // Then
        assertThat(conditionWithEpoch).contains("\"lastTransitionTime\":\"1970-01-01T00:00:00Z\"");
    }

    @Test
    void roundTrip() throws JsonProcessingException {
        // Given
        var conditionString = objectMapper.writeValueAsString(readyCondition);

        // When
        var readCondition = objectMapper.readValue(conditionString, Condition.class);

        // Then
        assertThat(readCondition).isEqualTo(readyCondition);
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

    @Test
    void shouldReturnTrueForResolveRefsFalse() {
        // Given
        Condition resolvedFalseCondition = new ConditionBuilder()
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withMessage("its no werkin")
                .withReason("BROKEN")
                .withObservedGeneration(345678L)
                .withLastTransitionTime(Instant.now())
                .build();

        // When
        boolean actual = Condition.isResolvedRefsFalse(resolvedFalseCondition);

        // Then
        assertThat(actual).isTrue();
    }

    @Test
    void shouldReturnFalseForResolveRefsTrue() {
        // Given
        Condition resolvedFalseCondition = new ConditionBuilder()
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.TRUE)
                .withMessage("its ALIVE")
                .withObservedGeneration(345678L)
                .withReason("FOUND_IT")
                .withLastTransitionTime(Instant.now())
                .build();

        // When
        boolean actual = Condition.isResolvedRefsFalse(resolvedFalseCondition);

        // Then
        assertThat(actual).isFalse();
    }

    @Test
    void shouldReturnFalseForReadyCondition() {
        // Given

        // When
        boolean actual = Condition.isResolvedRefsFalse(readyCondition);

        // Then
        assertThat(actual).isFalse();
    }
}