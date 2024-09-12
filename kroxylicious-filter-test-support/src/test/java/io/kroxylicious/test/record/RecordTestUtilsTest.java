/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.record;

import org.apache.kafka.common.record.MemoryRecords;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RecordTestUtilsTest {

    @Test
    void testMemoryRecordsWithAllRecordsDeleted() {
        MemoryRecords records = RecordTestUtils.memoryRecordsWithAllRecordsRemoved();
        assertThat(records.firstBatch()).isNotNull();
        assertThat(records.sizeInBytes()).isPositive();
        assertThat(records.records()).isEmpty();
    }

}
