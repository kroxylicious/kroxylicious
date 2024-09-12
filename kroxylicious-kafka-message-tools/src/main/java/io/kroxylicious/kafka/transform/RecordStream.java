/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * <p>An API for manipulating batches of {@link org.apache.kafka.common.record.Record}s.
 * This is loosely inspired by {@code java.util.stream.Stream},
 * but more sympathetic to Kafka's {@link org.apache.kafka.common.record.RecordBatch}
 * and {@link org.apache.kafka.common.record.Record} APIs.</p>
 *
 * <p>Conceptually a {@code RecordStream} is an ordered sequence of {@link org.apache.kafka.common.record.RecordBatch}es
 * with the ability to associate some state with records.</p>
 *
 * <h2>Map-like operations</h2>
 * <p>A stream created from {@link #ofRecords(MemoryRecords)} has no state associated with the records.
 * Methods are provided to map the state of streams</p>
 * <dl>
 * <dt>{@link #mapConstant(Object)}</dt>
 *   <dd>associates the same state with each record in the stream.</dd>
 * <dt>{@link #mapPerRecord(RecordMapper)}</dt>
 *   <dd>associates per-record state with each record in the stream.</dd>
 * </dl>
 *
 * <h2>Collect-like operations</h2>
 * <p>It is also possible to transform a stream to some non-stream representation:</p>
 * <dl>
 * <dt>{@link #toSet(RecordMapper)}</dt>
 *   <dd>to simultaneously map and convert a stream to a set.
 *       This is like {@link java.util.stream.Collectors#toMap(Function, Function)}.</dd>
 * <dt>{@link #toMemoryRecords(ByteBufferOutputStream, RecordTransform)}</dt>
 *   <dd>Simultaneously map and convert a stream to a {@link MemoryRecords}</dd>
 * </dl>
 * @param <T> The type of associated state.
 */
public class RecordStream<T> {

    private final MemoryRecords records;
    private final RecordMapper<Integer, T> stateFunction;

    private RecordStream(MemoryRecords records, RecordMapper<Integer, T> stateFunction) {
        this.records = records;
        this.stateFunction = stateFunction;
    }

    /**
     * Create a stream of the given {@code records}
     * @param records The records
     * @return A stream over those records.
     */
    public static RecordStream<Void> ofRecords(@NonNull
    MemoryRecords records) {
        Objects.requireNonNull(records);
        return new RecordStream<>(records, (batch, record, idx) -> null);
    }

    /**
     * Create a stream of the given {@code records} associating the records sequential index within
     * the given records as the per-record state.
     * @param records The records
     * @return A stream over those records.
     */
    public static RecordStream<Integer> ofRecordsWithIndex(@NonNull
    MemoryRecords records) {
        Objects.requireNonNull(records);
        return new RecordStream<>(records, (batch, record, idx) -> idx);
    }

    /**
     * Return a new stream of the records in this stream each associated with the given state.
     * @param state The state
     * @return A stream
     * @param <S> The type of state
     */
    public <S> RecordStream<S> mapConstant(S state) {
        return new RecordStream<>(records, (batch, record, idx) -> state);
    }

    /**
     * Return a new stream of the records in this stream each associated with the
     * state returned by the given {@code mapper} for that record.
     * @param mapper A function that returns the state to associate with a record.
     * @return A new stream
     * @param <S> The type of state
     */
    public <S> RecordStream<S> mapPerRecord(RecordMapper<T, S> mapper) {
        return new RecordStream<>(records, (batch, record, idx) -> mapper.apply(batch, record, stateFunction.apply(batch, record, idx)));
    }

    public void forEachRecord(RecordConsumer<T> mapper) {
        int i = 0;
        for (var batch : records.batches()) {
            if (!batch.isControlBatch()) {
                for (var record : batch) {
                    T existingState = stateFunction.apply(batch, record, i++);
                    mapper.accept(batch, record, existingState);
                }
            }
        }
    }

    /**
     * Map each of the records in this stream to some new state and return the set of those mapped states.
     * This iterates the batches in the source {@link MemoryRecords} and so will result in
     * batch decompression.
     * @param mapper The mapper function
     * @return The set
     * @param <S> The type of state
     */
    public <S> Set<S> toSet(RecordMapper<T, S> mapper) {
        return toCollection(mapper, new HashSet<>());
    }

    public <S> List<S> toList(RecordMapper<T, S> mapper) {
        return toCollection(mapper, new ArrayList<>());
    }

    private <C extends Collection<S>, S> C toCollection(RecordMapper<T, S> mapper, C result) {
        int i = 0;
        for (var batch : records.batches()) {
            if (!batch.isControlBatch()) {
                for (var record : batch) {
                    T existingState = stateFunction.apply(batch, record, i++);
                    result.add(mapper.apply(batch, record, existingState));
                }
            }
        }
        return result;
    }

    /**
     * Applies a {@link RecordTransform} to the records in this stream,
     * returning the mapped records in a {@link MemoryRecords}.
     * This method will preserve empty batches and control batches.
     * This iterates the batches in the source {@link MemoryRecords} and so will result in
     * batch decompression.
     *
     * @param transform The record transform
     * @return The mapped records
     */
    public MemoryRecords toMemoryRecords(
            @NonNull
            ByteBufferOutputStream buffer,
            @NonNull
            RecordTransform<T> transform
    ) {
        BatchAwareMemoryRecordsBuilder builder = new BatchAwareMemoryRecordsBuilder(buffer);
        int indexInStream = 0;
        for (var batch : records.batches()) {
            if (batch.isControlBatch()) {
                builder.writeBatch(batch);
            } else {
                int indexInBatch = 0;
                for (var record : batch) {
                    if (indexInBatch == 0) {
                        builder.addBatchLike(batch);
                        transform.initBatch(batch);
                    }
                    var existingState = stateFunction.apply(batch, record, indexInStream);
                    transform.init(existingState, record);
                    builder.appendWithOffset(
                            transform.transformOffset(record),
                            transform.transformTimestamp(record),
                            transform.transformKey(record),
                            transform.transformValue(record),
                            transform.transformHeaders(record)
                    );
                    transform.resetAfterTransform(existingState, record);
                    indexInStream++;
                    indexInBatch++;
                }
                if (indexInBatch == 0) { // batch was empty
                    builder.writeBatch(batch);
                }
            }
        }
        return builder.build();
    }

}
