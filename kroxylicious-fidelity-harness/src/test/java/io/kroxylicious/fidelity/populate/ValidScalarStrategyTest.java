/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import java.util.Random;

import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ValidScalarStrategyTest {

    private static final BoundField STRING_FIELD = new Schema(new Field("mechanism", Type.STRING, "doc")).get("mechanism");

    @Test
    void resolvesStringFieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(STRING_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(String.class));
    }

    @Test
    void sameSeedResolvesToSameValue() {
        // Given
        ValidScalarStrategy first = new ValidScalarStrategy(new Random(42));
        ValidScalarStrategy second = new ValidScalarStrategy(new Random(42));
        FieldDecision.Value firstDecision = (FieldDecision.Value) first.resolve(STRING_FIELD);

        // When
        FieldDecision.Value secondDecision = (FieldDecision.Value) second.resolve(STRING_FIELD);

        // Then
        assertThat(secondDecision.value()).isEqualTo(firstDecision.value());
    }
}
