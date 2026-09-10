/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.TaggedFields;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.internal.MemoryRecords;

/**
 * Populates scalar fields with valid random values, reproducible given the same seed.
 * <p>
 * Values are produced in Kafka's own type system, matching the schema vocabulary
 * {@link #resolve(BoundField)} already operates in - conversion to the Kroxylicious family, where a
 * setter needs it, is {@link SchemaDrivenMessagePopulator}'s concern, not this strategy's.
 */
public final class ValidScalarStrategy implements FieldPopulationStrategy {

    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final int MAX_STRING_LENGTH = 20;
    private static final int MAX_BYTES_LENGTH = 20;
    private static final int UINT16_BOUND = 1 << 16;
    private static final long UNSIGNED_INT32_BOUND = 1L << 32;
    private static final int MAX_ARRAY_LENGTH = 3;

    private final Random random;
    private final Map<Type, Supplier<Object>> suppliersByType;

    /**
     * Generate values using the provided random instance.
     * @param random the source of randomness; the caller owns the seed for reproducibility
     */
    public ValidScalarStrategy(Random random) {
        this.random = random;
        this.suppliersByType = createScalarSuppliers();
    }

    private Map<Type, Supplier<Object>> createScalarSuppliers() {
        Map<Type, Supplier<Object>> suppliers = new HashMap<>();
        suppliers.put(Type.STRING, this::randomString);
        suppliers.put(Type.COMPACT_STRING, this::randomString);
        suppliers.put(Type.COMPACT_NULLABLE_STRING, this::randomString);
        suppliers.put(Type.NULLABLE_STRING, this::randomString);
        suppliers.put(Type.BYTES, this::randomBytes);
        suppliers.put(Type.COMPACT_BYTES, this::randomBytes);
        suppliers.put(Type.COMPACT_NULLABLE_BYTES, this::randomBytes);
        suppliers.put(Type.NULLABLE_BYTES, this::randomBytes);
        suppliers.put(Type.RECORDS, () -> MemoryRecords.EMPTY);
        suppliers.put(Type.COMPACT_RECORDS, () -> MemoryRecords.EMPTY);
        suppliers.put(Type.COMPACT_NULLABLE_RECORDS, () -> MemoryRecords.EMPTY);
        suppliers.put(Type.NULLABLE_RECORDS, () -> MemoryRecords.EMPTY);
        suppliers.put(Type.UUID, this::randomUuid);
        suppliers.put(Type.FLOAT64, random::nextDouble);
        suppliers.put(Type.UINT16, () -> random.nextInt(UINT16_BOUND));
        suppliers.put(Type.UNSIGNED_INT32, () -> random.nextLong(UNSIGNED_INT32_BOUND));
        suppliers.put(Type.VARINT, random::nextInt);
        suppliers.put(Type.VARLONG, random::nextLong);
        suppliers.put(Type.INT32, random::nextInt);
        suppliers.put(Type.INT16, () -> (short) random.nextInt());
        suppliers.put(Type.INT8, () -> (byte) random.nextInt());
        suppliers.put(Type.INT64, random::nextLong);
        suppliers.put(Type.BOOLEAN, random::nextBoolean);
        return suppliers;
    }

    @Override
    public FieldDecision resolve(BoundField field) {
        if (field.def.type instanceof TaggedFields) {
            return new FieldDecision.Defer();
        }
        Optional<Type> arrayElementType = field.def.type.arrayElementType();
        Type leafType = arrayElementType.orElse(field.def.type);
        if (leafType instanceof Schema) {
            return new FieldDecision.Defer();
        }
        return arrayElementType.map(type -> new FieldDecision.Value(randomList(field.def.name, type)))
                .orElseGet(() -> new FieldDecision.Value(randomScalar(field.def.name, field.def.type)));
    }

    private Object randomScalar(String fieldName, Type type) {
        Supplier<Object> supplier = suppliersByType.get(type);
        if (supplier == null) {
            throw new UnsupportedOperationException("No valid-value strategy for field '" + fieldName + "'");
        }
        return supplier.get();
    }

    private List<Object> randomList(String fieldName, Type elementType) {
        int length = 1 + random.nextInt(MAX_ARRAY_LENGTH);
        List<Object> values = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            values.add(randomScalar(fieldName, elementType));
        }
        return values;
    }

    private String randomString() {
        int length = 1 + random.nextInt(MAX_STRING_LENGTH);
        StringBuilder value = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            value.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
        }
        return value.toString();
    }

    private byte[] randomBytes() {
        byte[] value = new byte[1 + random.nextInt(MAX_BYTES_LENGTH)];
        random.nextBytes(value);
        return value;
    }

    private Uuid randomUuid() {
        Uuid value = new Uuid(random.nextLong(), random.nextLong());
        while (Uuid.RESERVED.contains(value) || value.toString().startsWith("-")) {
            value = new Uuid(random.nextLong(), random.nextLong());
        }
        return value;
    }
}
