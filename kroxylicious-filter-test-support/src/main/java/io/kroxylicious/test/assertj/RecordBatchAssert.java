/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import java.util.Iterator;
import java.util.OptionalLong;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.api.IterableAssert;

import edu.umd.cs.findbugs.annotations.NonNull;

public class RecordBatchAssert extends AbstractAssert<RecordBatchAssert, RecordBatch> {
    protected RecordBatchAssert(RecordBatch batch) {
        super(batch, RecordBatchAssert.class);
        describedAs(batch == null ? "null record batch" : "record batch");
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

    public RecordBatchAssert hasBaseOffset(long expected) {
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
                  .isNotNull()
                  .isEqualTo(expected);
        return this;
    }

    public RecordBatchAssert hasNumRecords(int expected) {
        isNotNull();
        Assertions.assertThat(actual)
                  .describedAs("records")
                  .hasSize(expected);
        return this;
    }

    public RecordBatchAssert hasMagic(byte magic) {
        isNotNull();
        Assertions.assertThat(actual.magic())
                  .describedAs("magic")
                  .isEqualTo(magic);
        return this;
    }

    public RecordBatchAssert isControlBatch(boolean expected) {
        isNotNull();
        Assertions.assertThat(actual.isControlBatch())
                  .describedAs("controlBatch")
                  .isEqualTo(expected);
        return this;
    }

    public RecordBatchAssert isTransactional(boolean expected) {
        isNotNull();
        Assertions.assertThat(actual.isTransactional())
                  .describedAs("transactional")
                  .isEqualTo(expected);
        return this;
    }

    public RecordBatchAssert hasPartitionLeaderEpoch(int expected) {
        isNotNull();
        Assertions.assertThat(actual.partitionLeaderEpoch())
                  .describedAs("partitionLeaderEpoch")
                  .isEqualTo(expected);
        return this;
    }

    public RecordBatchAssert hasDeleteHorizonMs(OptionalLong expected) {
        isNotNull();
        Assertions.assertThat(actual.deleteHorizonMs())
                  .describedAs("deleteHorizonMs")
                  .isNotNull()
                  .isEqualTo(expected);
        return this;
    }

    public RecordBatchAssert hasLastOffset(long expected) {
        isNotNull();
        Assertions.assertThat(actual.lastOffset())
                  .describedAs("lastOffset")
                  .isEqualTo(expected);
        return this;
    }

    public RecordBatchAssert hasMetadataMatching(RecordBatch batch) {
        isNotNull();
        hasBaseOffset(batch.baseOffset());
        hasBaseSequence(batch.baseSequence());
        hasCompressionType(batch.compressionType());
        isControlBatch(batch.isControlBatch());
        isTransactional(batch.isTransactional());
        hasMagic(batch.magic());
        hasTimestampType(batch.timestampType());
        hasPartitionLeaderEpoch(batch.partitionLeaderEpoch());
        hasDeleteHorizonMs(batch.deleteHorizonMs());
        hasLastOffset(batch.lastOffset());
        hasMaxTimestamp(batch.maxTimestamp());
        hasProducerId(batch.producerId());
        hasProducerEpoch(batch.producerEpoch());
        hasLastSequence(batch.lastSequence());
        return this;
    }

    public RecordBatchAssert hasLastSequence(int expected) {
        isNotNull();
        Assertions.assertThat(actual.lastSequence())
                  .describedAs("lastSequence")
                  .isEqualTo(expected);
        return this;
    }

    public RecordBatchAssert hasProducerEpoch(short expected) {
        isNotNull();
        Assertions.assertThat(actual.producerEpoch())
                  .describedAs("producerEpoch")
                  .isEqualTo(expected);
        return this;
    }

    public RecordBatchAssert hasProducerId(long expected) {
        isNotNull();
        Assertions.assertThat(actual.producerId())
                  .describedAs("producerId")
                  .isEqualTo(expected);
        return this;
    }

    public RecordBatchAssert hasMaxTimestamp(long expected) {
        isNotNull();
        Assertions.assertThat(actual.maxTimestamp())
                  .describedAs("maxTimestamp")
                  .isEqualTo(expected);
        return this;
    }

    public RecordBatchAssert hasTimestampType(TimestampType expected) {
        isNotNull();
        Assertions.assertThat(actual.timestampType())
                  .describedAs("timestampType")
                  .isEqualTo(expected);
        return this;
    }

    private IterableAssert<Record> recordIterable() {
        isNotNull();
        IterableAssert<Record> records = IterableAssert.assertThatIterable(actual)
                                                       .describedAs("records");
        return records;
    }

    public RecordAssert firstRecord() {
        isNotNull();
        isNotEmpty();
        return recordIterable()
                               .first(new InstanceOfAssertFactory<>(Record.class, RecordAssert::assertThat))
                               .describedAs("first record");
    }

    public RecordAssert lastRecord() {
        isNotNull();
        isNotEmpty();
        return recordIterable()
                               .last(new InstanceOfAssertFactory<>(Record.class, RecordAssert::assertThat))
                               .describedAs("last record");
    }

    @NonNull
    private IterableAssert<Record> isNotEmpty() {
        return Assertions.assertThat(actual).describedAs(descriptionText()).hasSizeGreaterThan(0);
    }

    public Iterable<RecordAssert> records() {
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
