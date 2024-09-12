/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import edu.umd.cs.findbugs.annotations.NonNull;
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
 */
@NotThreadSafe
public interface RecordTransform<S> {

    void initBatch(@NonNull
    RecordBatch batch);

    void init(S state, @NonNull
    Record record);

    void resetAfterTransform(S state, @NonNull
    Record record);

    /**
     * @param record The operand record.
     * @return The offset of the new record.
     */
    long transformOffset(@NonNull
    Record record);

    /**
     * @param record The operand record.
     * @return The timestamp of the new record.
     */
    long transformTimestamp(@NonNull
    Record record);

    /**
     * @param record The operand record.
     * @return The key of the new record.
     */
    @Nullable
    ByteBuffer transformKey(@NonNull
    Record record);

    /**
     * @param record The operand record.
     * @return The value of the new record.
     */
    @Nullable
    ByteBuffer transformValue(@NonNull
    Record record);

    /**
     * @param record The operand record.
     * @return The headers of the new record. This may be null:
     * If the caller wants to create a new record from the result it must handle the fact that in
     * batch magic &gt;= 2 headers are required (but may be empty)
     * while in batch magic &lt; 2 headers are not permitted (thus must be null).
     */
    @Nullable
    Header[] transformHeaders(@NonNull
    Record record);
}
