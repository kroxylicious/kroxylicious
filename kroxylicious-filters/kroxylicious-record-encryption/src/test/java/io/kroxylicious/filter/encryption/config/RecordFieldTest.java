/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import java.util.EnumSet;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RecordFieldTest {

    @Test
    void serializationShouldRoundTrip() {
        for (var recordValue : List.of(
                EnumSet.of(RecordField.RECORD_VALUE),
                EnumSet.of(RecordField.RECORD_HEADER_VALUES),
                EnumSet.of(RecordField.RECORD_HEADER_VALUES, RecordField.RECORD_VALUE),
                EnumSet.allOf(RecordField.class),
                EnumSet.noneOf(RecordField.class)
        )) {
            var b = RecordField.toBits(recordValue);
            assertEquals(recordValue, RecordField.fromBits(b), recordValue.toString());
        }
    }

}
