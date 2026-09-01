/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform;

import java.nio.ByteBuffer;

import io.kroxylicious.kafka.common.header.Header;
import io.kroxylicious.kafka.common.record.internal.Record;
import io.kroxylicious.kafka.common.record.internal.RecordBatch;

import io.kroxylicious.proxy.tag.NotThreadSafe;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * <p>Represents a transformation from a {@link Record} to (the properties of) a new {@link Record}.
 *    {@code RecordTransform}s are stateful and retain ownership of the {@link ByteBuffer}s returned
 *    by their {@code transform*()} methods.</p>
 *
 * <p>When transforming a record callers of this interface must:</p>
 * <ol>
 *     <li>Transform one record at a time</li>
 *     <li>Invoke {@link #init(Object, Record)} for that record before any other methods</li>
 *     <li>Invoke the {@code transform*()} methods for that record, as required.
 *     They may be invoked zero, one or many times, and should be idempotent.
 *     They don't have to be invoked in any particular order.</li>
 *     <li>Invoke {@link #resetAfterTransform(Object, Record)} for that record</li>
 * </ol>
 *
 * @param <S> The type of state associated with a record.
 */
@NotThreadSafe
public interface RecordTransform<S> {

    /**
     * Called before any records from the given {@code batch} are transformed.
     *
     * @param batch The batch whose records are about to be transformed.
     */
    void initBatch(RecordBatch batch);

    /**
     * Prepares this transform for transforming the given {@code record}.
     * This is called once per record, before any of the {@code transform*()} methods
     * are invoked for that record.
     *
     * @param state The state associated with the record, may be null.
     * @param record The record about to be transformed.
     */
    void init(@Nullable S state, Record record);

    /**
     * Releases any resources associated with the transformation of the given {@code record}.
     * This is called once per record, after the {@code transform*()} methods
     * have been invoked for that record.
     *
     * @param state The state associated with the record.
     * @param record The record that was transformed.
     */
    void resetAfterTransform(S state, Record record);

    /**
     * Computes the offset for the transformed record.
     *
     * @param record The operand record.
     * @return The offset of the new record.
     */
    long transformOffset(Record record);

    /**
     * Computes the timestamp for the transformed record.
     *
     * @param record The operand record.
     * @return The timestamp of the new record.
     */
    long transformTimestamp(Record record);

    /**
     * Computes the key for the transformed record.
     *
     * @param record The operand record.
     * @return The key of the new record.
     */
    @Nullable
    ByteBuffer transformKey(Record record);

    /**
     * Computes the value for the transformed record.
     *
     * @param record The operand record.
     * @return The value of the new record.
     */
    @Nullable
    ByteBuffer transformValue(Record record);

    /**
     * Computes the headers for the transformed record.
     *
     * @param record The operand record.
     * @return The headers of the new record. This may be null:
     * If the caller wants to create a new record from the result it must handle the fact that in
     * batch magic &gt;= 2 headers are required (but may be empty)
     * while in batch magic &lt; 2 headers are not permitted (thus must be null).
     */
    @Nullable
    Header[] transformHeaders(Record record);
}
