/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.api.IterableAssert;

public class MemoryRecordsAssert extends AbstractAssert<MemoryRecordsAssert, MemoryRecords> {
    protected MemoryRecordsAssert(MemoryRecords memoryRecords) {
        super(memoryRecords, MemoryRecordsAssert.class);
        describedAs(memoryRecords == null ? "null memory records" : "memory records");
    }

    public static MemoryRecordsAssert assertThat(MemoryRecords actual) {
        return new MemoryRecordsAssert(actual);
    }

    public MemoryRecordsAssert hasSizeInBytes(int expected) {
        isNotNull();
        Assertions.assertThat(actual.sizeInBytes())
                  .describedAs("sizeInBytes")
                  .isEqualTo(expected);
        return this;
    }

    public IterableAssert<? extends RecordBatch> batchesIterable() {
        isNotNull();
        var batchesAssert = IterableAssert.assertThatIterable(actual.batches())
                                          .describedAs("batches");
        return batchesAssert;
    }

    public Iterable<RecordBatchAssert> batches() {
        isNotNull();
        return () -> {
            var it = actual.batches().iterator();
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public RecordBatchAssert next() {
                    return RecordBatchAssert.assertThat(it.next());
                }
            };
        };
    }

    public RecordBatchAssert firstBatch() {
        isNotNull();
        isNotEmpty();
        return batchesIterable()
                                .first(new InstanceOfAssertFactory<>(RecordBatch.class, RecordBatchAssert::assertThat))
                                .describedAs("first batch");
    }

    public RecordBatchAssert lastBatch() {
        isNotNull();
        isNotEmpty();
        return batchesIterable()
                                .last(new InstanceOfAssertFactory<>(RecordBatch.class, RecordBatchAssert::assertThat))
                                .describedAs("last batch");
    }

    private void isNotEmpty() {
        Assertions.assertThat(actual.batches())
                  .describedAs("number of batches")
                  .hasSizeGreaterThan(0);
    }

    public MemoryRecordsAssert hasNumBatches(int expected) {
        isNotNull();
        Assertions.assertThat(actual.batches())
                  .describedAs("number of batches")
                  .hasSize(expected);
        return this;
    }

    public MemoryRecordsAssert hasBatchSizes(int... expected) {
        isNotNull();
        List<Integer> actualCounts = new ArrayList<>();
        for (var batch : actual.batches()) {
            actualCounts.add((int) StreamSupport.stream(batch.spliterator(), false).count());
        }
        Assertions.assertThat(actualCounts)
                  .describedAs("batch sizes")
                  .isEqualTo(IntStream.of(expected).boxed().toList());
        return this;
    }

}
