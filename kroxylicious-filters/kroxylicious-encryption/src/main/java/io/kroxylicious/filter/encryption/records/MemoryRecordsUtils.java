/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.records;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
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
        Objects.requireNonNull(memoryRecords);
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
     * @param onClose A thing taking resposibility to execute the given clean-up tasks (e.g. in an exception handler).
     * @param resultBuffer A buffer
     */
    private static @NonNull Collector<MemoryRecords, BatchAwareMemoryRecordsBuilder, MemoryRecords> concatCollector(
                                                                                                                    @NonNull Consumer<Runnable> onClose,
                                                                                                                    @NonNull ByteBufferOutputStream resultBuffer) {
        /*
         * The finisher is not guaranteed to be called (e.g. in the event of an exception being thrown).
         * And for a parallel stream the finisher is only called for the final intermediate result, not for any
         * intermediates that arose from partitioning the underlying Spliterator.
         * So while, on the happy path, all intermediate BAMRB's will be close()'d by the combiner, and the
         * "final" BAMRB by the finisher, so guarantee cleanup in excpetional circumstances we
         * require the caller that invokes Stream#collect() to have a finally clause to execute all the Runnables
         * added via onClose.
         */
        Objects.requireNonNull(resultBuffer);
        return new Collector<>() {
            @Override
            public Supplier<BatchAwareMemoryRecordsBuilder> supplier() {
                return () -> {
                    BatchAwareMemoryRecordsBuilder batchAwareMemoryRecordsBuilder = new BatchAwareMemoryRecordsBuilder(resultBuffer);
                    onClose.accept(batchAwareMemoryRecordsBuilder::close);
                    return batchAwareMemoryRecordsBuilder;
                };
            }

            @Override
            public BiConsumer<BatchAwareMemoryRecordsBuilder, MemoryRecords> accumulator() {
                return MemoryRecordsUtils::appendAll;
            }

            @Override
            public BinaryOperator<BatchAwareMemoryRecordsBuilder> combiner() {
                return MemoryRecordsUtils::combineBuilders;
            }

            @Override
            public Function<BatchAwareMemoryRecordsBuilder, MemoryRecords> finisher() {
                return BatchAwareMemoryRecordsBuilder::build;
            }

            @Override
            public Set<Characteristics> characteristics() {
                return Set.of();
            }
        };
    }

    /**
     * Concatenate the memory records in the given {@code stream} into one,
     * using the given {@code buffer}.
     * @param stream The memory records to concatenate
     * @param buffer A temporary buffer to use (to encourage buffer reuse.
     * @return A memory records containing the same batches and returns as were encountered in the given {@code stream}.
     */
    public static @NonNull MemoryRecords concat(@NonNull Stream<MemoryRecords> stream, @NonNull ByteBufferOutputStream buffer) {
        Objects.requireNonNull(stream);
        Objects.requireNonNull(buffer);
        var cleanUpTasks = new ArrayList<Runnable>(1);
        try (stream) {
            return stream.sequential().collect(concatCollector(cleanUpTasks::add, buffer));
        }
        finally {
            cleanUpTasks.forEach(Runnable::run);
        }
    }
}
