/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.TaggedFields;
import org.apache.kafka.common.protocol.types.Type;

import io.kroxylicious.kafka.common.Uuid;
import io.kroxylicious.kafka.common.record.internal.MemoryRecords;

/**
 * Populates scalar fields with valid random values, reproducible given the same seed.
 */
public final class ValidScalarStrategy implements FieldPopulationStrategy {

    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final int MAX_STRING_LENGTH = 20;
    private static final int MAX_BYTES_LENGTH = 20;
    private static final int UINT16_BOUND = 1 << 16;
    private static final long UNSIGNED_INT32_BOUND = 1L << 32;
    private static final int MAX_ARRAY_LENGTH = 3;

    private final Random random;

    /**
     * Generate values using the provided random instance.
     * @param random the source of randomness; the caller owns the seed for reproducibility
     */
    public ValidScalarStrategy(Random random) {
        this.random = random;
    }

    @Override
    public FieldDecision resolve(BoundField field) {
        if (field.def.type instanceof TaggedFields) {
            return new FieldDecision.Defer();
        }
        if (field.def.type.arrayElementType().isPresent()) {
            return new FieldDecision.Value(randomList(field.def.type.arrayElementType().get()));
        }
        return new FieldDecision.Value(randomScalar(field.def.type));
    }

    private Object randomScalar(Type type) {
        if (type.equals(Type.STRING)) {
            return randomString();
        }
        if (type.equals(Type.BYTES)) {
            return randomBytes();
        }
        if (type.equals(Type.COMPACT_BYTES)) {
            return randomBytes();
        }
        if (type.equals(Type.COMPACT_NULLABLE_BYTES)) {
            return randomBytes();
        }
        if (type.equals(Type.COMPACT_NULLABLE_RECORDS)) {
            return MemoryRecords.EMPTY;
        }
        if (type.equals(Type.COMPACT_NULLABLE_STRING)) {
            return randomString();
        }
        if (type.equals(Type.COMPACT_RECORDS)) {
            return MemoryRecords.EMPTY;
        }
        if (type.equals(Type.COMPACT_STRING)) {
            return randomString();
        }
        if (type.equals(Type.FLOAT64)) {
            return random.nextDouble();
        }
        if (type.equals(Type.NULLABLE_BYTES)) {
            return randomBytes();
        }
        if (type.equals(Type.NULLABLE_RECORDS)) {
            return MemoryRecords.EMPTY;
        }
        if (type.equals(Type.NULLABLE_STRING)) {
            return randomString();
        }
        if (type.equals(Type.RECORDS)) {
            return MemoryRecords.EMPTY;
        }
        if (type.equals(Type.UINT16)) {
            return random.nextInt(UINT16_BOUND);
        }
        if (type.equals(Type.UNSIGNED_INT32)) {
            return random.nextLong(UNSIGNED_INT32_BOUND);
        }
        if (type.equals(Type.UUID)) {
            return randomUuid();
        }
        if (type.equals(Type.VARINT)) {
            return random.nextInt();
        }
        if (type.equals(Type.VARLONG)) {
            return random.nextLong();
        }
        if (type.equals(Type.INT32)) {
            return random.nextInt();
        }
        if (type.equals(Type.INT16)) {
            return (short) random.nextInt();
        }
        if (type.equals(Type.INT8)) {
            return (byte) random.nextInt();
        }
        if (type.equals(Type.INT64)) {
            return random.nextLong();
        }
        if (type.equals(Type.BOOLEAN)) {
            return random.nextBoolean();
        }
        throw new UnsupportedOperationException("No valid-value strategy for type " + type);
    }

    private List<Object> randomList(Type elementType) {
        int length = 1 + random.nextInt(MAX_ARRAY_LENGTH);
        List<Object> values = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            values.add(randomScalar(elementType));
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
