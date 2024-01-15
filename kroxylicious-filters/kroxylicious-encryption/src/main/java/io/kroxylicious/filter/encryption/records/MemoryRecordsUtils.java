/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.records;

import java.util.Spliterator;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Utility methods for dealing with {@link MemoryRecords} and {@link MemoryRecordsBuilder}s.
 * @see RecordBatchUtils
 */
public class MemoryRecordsUtils {
    private MemoryRecordsUtils() {
    }

    /**
     * Returns a sequential stream over the batches in a {@link MemoryRecords}.
     * @param memoryRecords The memoryRecords
     * @return A stream over the batches in the given {@code memoryRecords}.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static @NonNull Stream<RecordBatch> batchStream(@NonNull MemoryRecords memoryRecords) {
        return StreamSupport.<RecordBatch> stream(
                () -> (Spliterator) memoryRecords.batches().spliterator(),
                Spliterator.ORDERED | Spliterator.NONNULL,
                false);
    }

    /**
     * Append all the records in the given {@code memoryRecords} to the given {@code memoryRecordsBuilder}
     * @param memoryRecordsBuilder The build to append to
     * @param memoryRecords The source of the records
     * @return The given {@code memoryRecordsBuilder}.
     */
    private static BatchAwareMemoryRecordsBuilder appendAll(BatchAwareMemoryRecordsBuilder memoryRecordsBuilder, MemoryRecords memoryRecords) {
        for (var batch : memoryRecords.batches()) {
            memoryRecordsBuilder.addBatchLike(batch);
            for (var record : batch) {
                memoryRecordsBuilder.append(record);
            }
        }
        return memoryRecordsBuilder;
    }

    /**
     * A {@link Collector} "combiner" function for {@link MemoryRecordsBuilder}s.
     * The records from both arguments will be in the same batch.
     */
    /* test */ static BatchAwareMemoryRecordsBuilder combineBuilders(BatchAwareMemoryRecordsBuilder mrb1, BatchAwareMemoryRecordsBuilder mrb2) {
        return appendAll(mrb1, mrb2.build());
    }

    /**
     * Factory method for a Collector that concatenates MemoryRecords.
     */
    public static @NonNull Collector<MemoryRecords, BatchAwareMemoryRecordsBuilder, MemoryRecords> concatCollector(@NonNull ByteBufferOutputStream resultBuffer) {
        return Collector.of(
                () -> new BatchAwareMemoryRecordsBuilder(resultBuffer),
                MemoryRecordsUtils::appendAll,
                MemoryRecordsUtils::combineBuilders,
                BatchAwareMemoryRecordsBuilder::build);
    }
}
