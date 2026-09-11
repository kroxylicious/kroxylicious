/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.filter.assertj;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.api.IterableAssert;

import io.kroxylicious.kafka.common.record.internal.MemoryRecords;
import io.kroxylicious.kafka.common.record.internal.RecordBatch;

/**
 * AssertJ assertions for {@link MemoryRecords}.
 */
public class MemoryRecordsAssert extends AbstractAssert<MemoryRecordsAssert, MemoryRecords> {
    /**
     * Constructs an assertion for the given MemoryRecords.
     *
     * @param memoryRecords the actual MemoryRecords
     */
    protected MemoryRecordsAssert(MemoryRecords memoryRecords) {
        super(memoryRecords, MemoryRecordsAssert.class);
        describedAs(memoryRecords == null ? "null memory records" : "memory records");
    }

    /**
     * Creates an assertion for the given MemoryRecords.
     *
     * @param actual the actual MemoryRecords
     * @return the assertion
     */
    public static MemoryRecordsAssert assertThat(MemoryRecords actual) {
        return new MemoryRecordsAssert(actual);
    }

    /**
     * Verifies that the MemoryRecords have the expected size in bytes.
     *
     * @param expected the expected size in bytes
     * @return this assertion
     */
    public MemoryRecordsAssert hasSizeInBytes(int expected) {
        isNotNull();
        Assertions.assertThat(actual.sizeInBytes())
                .describedAs("sizeInBytes")
                .isEqualTo(expected);
        return this;
    }

    /**
     * Creates an iterable assertion over the batches.
     *
     * @return the batches assertion
     */
    public IterableAssert<? extends RecordBatch> batchesIterable() {
        isNotNull();
        return IterableAssert.assertThatIterable(actual.batches())
                .describedAs("batches");
    }

    /**
     * Returns an iterable of assertions, one for each batch.
     *
     * @return the batch assertions
     */
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

    /**
     * Verifies that there is at least one batch and creates an assertion for the first one.
     *
     * @return the first batch assertion
     */
    public RecordBatchAssert firstBatch() {
        isNotNull();
        isNotEmpty();
        return batchesIterable()
                .first(new InstanceOfAssertFactory<>(RecordBatch.class, RecordBatchAssert::assertThat))
                .describedAs("first batch");
    }

    /**
     * Verifies that there is at least one batch and creates an assertion for the last one.
     *
     * @return the last batch assertion
     */
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

    /**
     * Verifies that the MemoryRecords contain the expected number of batches.
     *
     * @param expected the expected number of batches
     * @return this assertion
     */
    public MemoryRecordsAssert hasNumBatches(int expected) {
        isNotNull();
        Assertions.assertThat(actual.batches())
                .describedAs("number of batches")
                .hasSize(expected);
        return this;
    }

    /**
     * Verifies that the batches contain the expected numbers of records.
     *
     * @param expected the expected number of records of each batch, in order
     * @return this assertion
     */
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
