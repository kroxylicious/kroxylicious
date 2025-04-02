/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

class ConditionTest {

    @Test
    void serializedFormShouldUseIso8601() throws JsonProcessingException {
        // Given
        Condition build = new ConditionBuilder().withLastTransitionTime(Instant.EPOCH).build();
        // When
        var conditionWithEpoch = new ObjectMapper().writeValueAsString(build);
        // Then
        assertThat(conditionWithEpoch).isEqualTo("{\"lastTransitionTime\":\"1970-01-01T00:00:00Z\"}");
    }

    @Test
    void roundTrip() throws JsonProcessingException {
        // Given
        ObjectMapper objectMapper = new ObjectMapper();
        Condition wroteCondition = new ConditionBuilder().withLastTransitionTime(Instant.EPOCH).build();
        // When
        var conditionString = objectMapper.writeValueAsString(wroteCondition);
        var readCondition = objectMapper.readValue(conditionString, Condition.class);
        // Then
        assertThat(readCondition).isEqualTo(wroteCondition);
    }

}
