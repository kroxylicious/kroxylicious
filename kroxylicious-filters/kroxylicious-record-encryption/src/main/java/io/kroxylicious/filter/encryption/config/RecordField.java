/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import java.util.EnumSet;
import java.util.Set;

/**
 * Enumerates the parts of a Kafka Record that can be encrypted.
 */
public enum RecordField {
    /** The value of the record. */
    RECORD_VALUE((byte) 1),
    /** The values of the record's headers. */
    RECORD_HEADER_VALUES((byte) (1 << 1));

    private final byte code;

    // RECORD_KEY((byte) (1 << 2)),
    // RECORD_TIMESTAMP((byte) (1 << 3)
    RecordField(byte code) {
        this.code = code;
    }

    /**
     * Serializes the given set of record fields to a bitset.
     * @param recordField the record fields to serialize.
     * @return the bitset representing the given record fields.
     */
    public static byte toBits(Set<RecordField> recordField) {
        return (byte) recordField.stream()
                .mapToInt(w -> w.code).reduce((x, y) -> x | y)
                .orElse(0);
    }

    /**
     * Deserializes a set of record fields from the given bitset.
     * @param b the bitset representing a set of record fields.
     * @return the set of record fields represented by the given bitset.
     */
    public static Set<RecordField> fromBits(byte b) {
        var result = EnumSet.noneOf(RecordField.class);
        if ((b & RecordField.RECORD_VALUE.code) != 0) {
            result.add(RecordField.RECORD_VALUE);
        }
        if ((b & RecordField.RECORD_HEADER_VALUES.code) != 0) {
            result.add(RecordField.RECORD_HEADER_VALUES);
        }
        return result;
    }
}
