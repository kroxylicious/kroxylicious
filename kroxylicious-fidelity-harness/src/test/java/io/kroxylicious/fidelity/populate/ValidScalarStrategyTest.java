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
    private static final BoundField BYTES_FIELD = new Schema(new Field("auth_bytes", Type.BYTES, "doc")).get("auth_bytes");
    private static final BoundField INT32_FIELD = new Schema(new Field("generation_id", Type.INT32, "doc")).get("generation_id");
    private static final BoundField INT16_FIELD = new Schema(new Field("error_code", Type.INT16, "doc")).get("error_code");
    private static final BoundField INT8_FIELD = new Schema(new Field("key_type", Type.INT8, "doc")).get("key_type");

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
    void resolvesBytesFieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(BYTES_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(byte[].class));
    }

    @Test
    void resolvesInt32FieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(INT32_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(Integer.class));
    }

    @Test
    void resolvesInt16FieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(INT16_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(Short.class));
    }

    @Test
    void resolvesInt8FieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(INT8_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(Byte.class));
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
