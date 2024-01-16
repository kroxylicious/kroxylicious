/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

public class KafkaAssertions {
    private KafkaAssertions() {
    }

    // Helper methods for the package

    static String bufferToString(ByteBuffer bb) {
        if (bb == null) {
            return null;
        }
        String result = StandardCharsets.UTF_8.decode(bb).toString();
        bb.flip();
        return result;
    }

    // Assertions

    public static MemoryRecordsAssert assertThat(MemoryRecords actual) {
        return MemoryRecordsAssert.assertThat(actual);
    }

    public static RecordBatchAssert assertThat(RecordBatch actual) {
        return RecordBatchAssert.assertThat(actual);
    }

    public static RecordAssert assertThat(Record actual) {
        return RecordAssert.assertThat(actual);
    }

    public static HeaderAssert assertThat(Header actual) {
        return HeaderAssert.assertThat(actual);
    }

}
