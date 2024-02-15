/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.records;

import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

@FunctionalInterface
public interface RecordMapper<S, T> {

    T apply(RecordBatch batch, Record record, S state);
}
