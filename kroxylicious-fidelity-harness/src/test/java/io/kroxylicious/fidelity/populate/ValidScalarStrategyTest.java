/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import java.util.Random;

import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kafka.common.record.internal.MemoryRecords;

import static org.assertj.core.api.Assertions.assertThat;

class ValidScalarStrategyTest {

    private static final BoundField STRING_FIELD = new Schema(new Field("mechanism", Type.STRING, "doc")).get("mechanism");
    private static final BoundField BYTES_FIELD = new Schema(new Field("auth_bytes", Type.BYTES, "doc")).get("auth_bytes");
    private static final BoundField INT32_FIELD = new Schema(new Field("generation_id", Type.INT32, "doc")).get("generation_id");
    private static final BoundField INT16_FIELD = new Schema(new Field("error_code", Type.INT16, "doc")).get("error_code");
    private static final BoundField INT8_FIELD = new Schema(new Field("key_type", Type.INT8, "doc")).get("key_type");
    private static final BoundField INT64_FIELD = new Schema(new Field("producer_id", Type.INT64, "doc")).get("producer_id");
    private static final BoundField BOOLEAN_FIELD = new Schema(new Field("committed", Type.BOOLEAN, "doc")).get("committed");
    private static final BoundField EMPTY_TAGGED_FIELDS_SECTION = new Schema(TaggedFieldsSection.of()).get("_tagged_fields");
    private static final BoundField COMPACT_BYTES_FIELD = new Schema(new Field("request_data", Type.COMPACT_BYTES, "doc")).get("request_data");
    private static final BoundField COMPACT_NULLABLE_BYTES_FIELD = new Schema(new Field("response_data", Type.COMPACT_NULLABLE_BYTES, "doc")).get("response_data");
    private static final BoundField COMPACT_NULLABLE_RECORDS_FIELD = new Schema(new Field("records", Type.COMPACT_NULLABLE_RECORDS, "doc")).get("records");
    private static final BoundField COMPACT_NULLABLE_STRING_FIELD = new Schema(new Field("error_message", Type.COMPACT_NULLABLE_STRING, "doc")).get("error_message");
    private static final BoundField COMPACT_RECORDS_FIELD = new Schema(new Field("records", Type.COMPACT_RECORDS, "doc")).get("records");
    private static final BoundField COMPACT_STRING_FIELD = new Schema(new Field("client_software_name", Type.COMPACT_STRING, "doc")).get("client_software_name");
    private static final BoundField FLOAT64_FIELD = new Schema(new Field("value", Type.FLOAT64, "doc")).get("value");
    private static final BoundField NULLABLE_BYTES_FIELD = new Schema(new Field("user_data", Type.NULLABLE_BYTES, "doc")).get("user_data");
    private static final BoundField NULLABLE_RECORDS_FIELD = new Schema(new Field("records", Type.NULLABLE_RECORDS, "doc")).get("records");
    private static final BoundField NULLABLE_STRING_FIELD = new Schema(new Field("client_id", Type.NULLABLE_STRING, "doc")).get("client_id");

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
    void resolvesInt64FieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(INT64_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(Long.class));
    }

    @Test
    void resolvesBooleanFieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(BOOLEAN_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(Boolean.class));
    }

    @Test
    void resolvesEmptyTaggedFieldsSectionToDefer() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(EMPTY_TAGGED_FIELDS_SECTION);

        // Then
        assertThat(decision).isInstanceOf(FieldDecision.Defer.class);
    }

    @Test
    void resolvesCompactBytesFieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(COMPACT_BYTES_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(byte[].class));
    }

    @Test
    void resolvesCompactNullableBytesFieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(COMPACT_NULLABLE_BYTES_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(byte[].class));
    }

    @Test
    void resolvesCompactNullableRecordsFieldToEmptyRecords() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(COMPACT_NULLABLE_RECORDS_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isSameAs(MemoryRecords.EMPTY));
    }

    @Test
    void resolvesCompactNullableStringFieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(COMPACT_NULLABLE_STRING_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(String.class));
    }

    @Test
    void resolvesCompactRecordsFieldToEmptyRecords() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(COMPACT_RECORDS_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isSameAs(MemoryRecords.EMPTY));
    }

    @Test
    void resolvesCompactStringFieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(COMPACT_STRING_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(String.class));
    }

    @Test
    void resolvesFloat64FieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(FLOAT64_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(Double.class));
    }

    @Test
    void resolvesNullableBytesFieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(NULLABLE_BYTES_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(byte[].class));
    }

    @Test
    void resolvesNullableRecordsFieldToEmptyRecords() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(NULLABLE_RECORDS_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isSameAs(MemoryRecords.EMPTY));
    }

    @Test
    void resolvesNullableStringFieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(NULLABLE_STRING_FIELD);

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
