/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import java.util.List;
import java.util.Random;

import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kafka.common.Uuid;
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
    private static final BoundField RECORDS_FIELD = new Schema(new Field("records", Type.RECORDS, "doc")).get("records");
    private static final BoundField UINT16_FIELD = new Schema(new Field("port", Type.UINT16, "doc")).get("port");
    private static final BoundField UNSIGNED_INT32_FIELD = new Schema(new Field("value", Type.UNSIGNED_INT32, "doc")).get("value");
    private static final BoundField UUID_FIELD = new Schema(new Field("voter_directory_id", Type.UUID, "doc")).get("voter_directory_id");
    private static final BoundField VARINT_FIELD = new Schema(new Field("value", Type.VARINT, "doc")).get("value");
    private static final BoundField VARLONG_FIELD = new Schema(new Field("value", Type.VARLONG, "doc")).get("value");
    private static final BoundField STRING_ARRAY_FIELD = new Schema(new Field("topic_names", new ArrayOf(Type.STRING), "doc")).get("topic_names");

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
    void resolvesRecordsFieldToEmptyRecords() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(RECORDS_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isSameAs(MemoryRecords.EMPTY));
    }

    @Test
    void resolvesUint16FieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(UINT16_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(Integer.class));
    }

    @Test
    void resolvesUnsignedInt32FieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(UNSIGNED_INT32_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(Long.class));
    }

    @Test
    void resolvesUuidFieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(UUID_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(Uuid.class));
    }

    @Test
    void resolvesVarintFieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(VARINT_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(Integer.class));
    }

    @Test
    void resolvesVarlongFieldToNonNullValue() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(VARLONG_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> assertThat(value.value()).isInstanceOf(Long.class));
    }

    @Test
    void resolvesArrayFieldToListOfElementValues() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));

        // When
        FieldDecision decision = strategy.resolve(STRING_ARRAY_FIELD);

        // Then
        assertThat(decision).isInstanceOfSatisfying(FieldDecision.Value.class, value -> {
            assertThat(value.value()).isInstanceOf(List.class);
            List<?> elements = (List<?>) value.value();
            assertThat(elements).isNotEmpty();
            assertThat(elements).allSatisfy(element -> assertThat(element).isInstanceOf(String.class));
        });
    }

    @Test
    void resolvesStructFieldToDefer() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));
        Schema elementSchema = new Schema(new Field("value", Type.INT32, "doc"));
        BoundField structField = new Schema(new Field("result", elementSchema, "doc")).get("result");

        // When
        FieldDecision decision = strategy.resolve(structField);

        // Then
        assertThat(decision).isInstanceOf(FieldDecision.Defer.class);
    }

    @Test
    void resolvesStructArrayFieldToDefer() {
        // Given
        ValidScalarStrategy strategy = new ValidScalarStrategy(new Random(42));
        Schema elementSchema = new Schema(new Field("value", Type.INT32, "doc"));
        BoundField structArrayField = new Schema(new Field("results", new ArrayOf(elementSchema), "doc")).get("results");

        // When
        FieldDecision decision = strategy.resolve(structArrayField);

        // Then
        assertThat(decision).isInstanceOf(FieldDecision.Defer.class);
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
