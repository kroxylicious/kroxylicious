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
    RECORD_VALUE((byte) 1),
    RECORD_HEADER_VALUES((byte) (1 << 1));

    private final byte code;

    // RECORD_KEY((byte) (1 << 2)),
    // RECORD_TIMESTAMP((byte) (1 << 3)
    RecordField(byte code) {
        this.code = code;
    }

    public static byte toBits(Set<RecordField> recordField) {
        return (byte) recordField.stream()
                                 .mapToInt(w -> w.code)
                                 .reduce((x, y) -> x | y)
                                 .orElse(0);
    }

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
