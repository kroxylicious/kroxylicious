/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import java.util.Iterator;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.api.IterableAssert;

public class RecordBatchAssert extends AbstractAssert<RecordBatchAssert, RecordBatch> {
    protected RecordBatchAssert(RecordBatch batch) {
        super(batch, RecordBatchAssert.class);
    }

    public static RecordBatchAssert assertThat(RecordBatch actual) {
        return new RecordBatchAssert(actual);
    }

    public RecordBatchAssert hasSizeInBytes(int expected) {
        isNotNull();
        Assertions.assertThat(actual.sizeInBytes())
                .describedAs("sizeInBytes")
                .isEqualTo(expected);
        return this;
    }

    public RecordBatchAssert hasBaseOffset(int expected) {
        isNotNull();
        Assertions.assertThat(actual.baseOffset())
                .describedAs("baseOffset")
                .isEqualTo(expected);
        return this;
    }

    public RecordBatchAssert hasBaseSequence(int expected) {
        isNotNull();
        Assertions.assertThat(actual.baseSequence())
                .describedAs("baseSequence")
                .isEqualTo(expected);
        return this;
    }

    public RecordBatchAssert hasCompressionType(CompressionType expected) {
        isNotNull();
        Assertions.assertThat(actual.compressionType())
                .describedAs("compressionType")
                .isEqualTo(expected);
        return this;
    }

    public RecordBatchAssert hasNumRecords(int expected) {
        Assertions.assertThat(actual)
                .describedAs("records")
                .hasSize(expected);
        return this;
    }

    private IterableAssert<Record> recordIterable() {
        isNotNull();
        IterableAssert<Record> records = IterableAssert.assertThatIterable(actual)
                .describedAs("records");
        return records;
    }

    public RecordAssert firstRecord() {
        return recordIterable()
                .first(new InstanceOfAssertFactory<>(Record.class, RecordAssert::assertThat))
                .describedAs("first record");
    }

    public RecordAssert lastRecord() {
        return recordIterable()
                .last(new InstanceOfAssertFactory<>(Record.class, RecordAssert::assertThat))
                .describedAs("last record");
    }

    public Iterable<RecordAssert> records() {
        recordIterable().isNotEmpty();
        isNotNull();
        return () -> {
            return new Iterator<RecordAssert>() {
                Iterator<Record> it = actual.iterator();
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public RecordAssert next() {
                    return RecordAssert.assertThat(it.next());
                }
            };
        };
    }
}
