/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform;

import io.kroxylicious.kafka.common.record.internal.Record;
import io.kroxylicious.kafka.common.record.internal.RecordBatch;

/**
 * A consumer of {@link Record}s, together with the {@link RecordBatch} that contains them
 * and any state associated with the record.
 *
 * @param <S> The type of state associated with a record.
 */
@FunctionalInterface
public interface RecordConsumer<S> {

    /**
     * Consumes the given record.
     *
     * @param batch The batch containing the record.
     * @param record The record.
     * @param state The state associated with the record.
     */
    void accept(RecordBatch batch, Record record, S state);
}
