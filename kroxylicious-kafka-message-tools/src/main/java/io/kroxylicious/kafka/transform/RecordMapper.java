/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform;

import io.kroxylicious.kafka.common.record.internal.Record;
import io.kroxylicious.kafka.common.record.internal.RecordBatch;

/**
 * A function that maps a {@link Record}, together with the {@link RecordBatch} that contains it
 * and any state associated with the record, to some result.
 *
 * @param <S> The type of state associated with a record.
 * @param <T> The type of the result of the mapping.
 */
@FunctionalInterface
public interface RecordMapper<S, T> {

    /**
     * Maps the given record to a result.
     *
     * @param batch The batch containing the record.
     * @param record The record.
     * @param state The state associated with the record.
     * @return The result of the mapping.
     */
    T apply(RecordBatch batch, Record record, S state);
}
