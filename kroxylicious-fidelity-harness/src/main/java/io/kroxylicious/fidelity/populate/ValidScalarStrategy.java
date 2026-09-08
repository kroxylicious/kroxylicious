/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import java.util.Random;

import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.TaggedFields;
import org.apache.kafka.common.protocol.types.Type;

/**
 * Populates scalar fields with valid random values, reproducible given the same seed.
 */
public final class ValidScalarStrategy implements FieldPopulationStrategy {

    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final int MAX_STRING_LENGTH = 20;
    private static final int MAX_BYTES_LENGTH = 20;

    private final Random random;

    /**
     * @param random the source of randomness; the caller owns the seed for reproducibility
     */
    public ValidScalarStrategy(Random random) {
        this.random = random;
    }

    @Override
    public FieldDecision resolve(BoundField field) {
        if (field.def.type.equals(Type.STRING)) {
            return new FieldDecision.Value(randomString());
        }
        if (field.def.type.equals(Type.BYTES)) {
            return new FieldDecision.Value(randomBytes());
        }
        if (field.def.type.equals(Type.COMPACT_BYTES)) {
            return new FieldDecision.Value(randomBytes());
        }
        if (field.def.type.equals(Type.INT32)) {
            return new FieldDecision.Value(random.nextInt());
        }
        if (field.def.type.equals(Type.INT16)) {
            return new FieldDecision.Value((short) random.nextInt());
        }
        if (field.def.type.equals(Type.INT8)) {
            return new FieldDecision.Value((byte) random.nextInt());
        }
        if (field.def.type.equals(Type.INT64)) {
            return new FieldDecision.Value(random.nextLong());
        }
        if (field.def.type.equals(Type.BOOLEAN)) {
            return new FieldDecision.Value(random.nextBoolean());
        }
        if (field.def.type instanceof TaggedFields) {
            return new FieldDecision.Defer();
        }
        throw new UnsupportedOperationException("No valid-value strategy for type " + field.def.type);
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
}
