/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.filter.assertj;

import java.util.Iterator;
import java.util.OptionalLong;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.api.IterableAssert;

import io.kroxylicious.kafka.common.record.TimestampType;
import io.kroxylicious.kafka.common.record.internal.CompressionType;
import io.kroxylicious.kafka.common.record.internal.Record;
import io.kroxylicious.kafka.common.record.internal.RecordBatch;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * AssertJ assertions for {@link RecordBatch}.
 */
public class RecordBatchAssert extends AbstractAssert<RecordBatchAssert, RecordBatch> {
    /**
     * Constructs an assertion for the given RecordBatch.
     *
     * @param batch the actual RecordBatch
     */
    protected RecordBatchAssert(RecordBatch batch) {
        super(batch, RecordBatchAssert.class);
        describedAs(batch == null ? "null record batch" : "record batch");
    }

    /**
     * Creates an assertion for the given RecordBatch.
     *
     * @param actual the actual RecordBatch
     * @return the assertion
     */
    public static RecordBatchAssert assertThat(RecordBatch actual) {
        return new RecordBatchAssert(actual);
    }

    /**
     * Verifies that the batch has the expected size in bytes.
     *
     * @param expected the expected size in bytes
     * @return this assertion
     */
    public RecordBatchAssert hasSizeInBytes(int expected) {
        isNotNull();
        Assertions.assertThat(actual.sizeInBytes())
                .describedAs("sizeInBytes")
                .isEqualTo(expected);
        return this;
    }

    /**
     * Verifies that the batch has the expected base offset.
     *
     * @param expected the expected base offset
     * @return this assertion
     */
    public RecordBatchAssert hasBaseOffset(long expected) {
        isNotNull();
        Assertions.assertThat(actual.baseOffset())
                .describedAs("baseOffset")
                .isEqualTo(expected);
        return this;
    }

    /**
     * Verifies that the batch has the expected base sequence.
     *
     * @param expected the expected base sequence
     * @return this assertion
     */
    public RecordBatchAssert hasBaseSequence(int expected) {
        isNotNull();
        Assertions.assertThat(actual.baseSequence())
                .describedAs("baseSequence")
                .isEqualTo(expected);
        return this;
    }

    /**
     * Verifies that the batch has the expected compression type.
     *
     * @param expected the expected compression type
     * @return this assertion
     */
    public RecordBatchAssert hasCompressionType(CompressionType expected) {
        isNotNull();
        Assertions.assertThat(actual.compressionType())
                .describedAs("compressionType")
                .isNotNull()
                .isEqualTo(expected);
        return this;
    }

    /**
     * Verifies that the batch contains the expected number of records.
     *
     * @param expected the expected number of records
     * @return this assertion
     */
    public RecordBatchAssert hasNumRecords(int expected) {
        isNotNull();
        Assertions.assertThat(actual)
                .describedAs("records")
                .hasSize(expected);
        return this;
    }

    /**
     * Verifies that the batch has the expected magic byte.
     *
     * @param magic the expected magic byte
     * @return this assertion
     */
    public RecordBatchAssert hasMagic(byte magic) {
        isNotNull();
        Assertions.assertThat(actual.magic())
                .describedAs("magic")
                .isEqualTo(magic);
        return this;
    }

    /**
     * Verifies whether the batch is a control batch.
     *
     * @param expected whether the batch is expected to be a control batch
     * @return this assertion
     */
    public RecordBatchAssert isControlBatch(boolean expected) {
        isNotNull();
        Assertions.assertThat(actual.isControlBatch())
                .describedAs("controlBatch")
                .isEqualTo(expected);
        return this;
    }

    /**
     * Verifies whether the batch is transactional.
     *
     * @param expected whether the batch is expected to be transactional
     * @return this assertion
     */
    public RecordBatchAssert isTransactional(boolean expected) {
        isNotNull();
        Assertions.assertThat(actual.isTransactional())
                .describedAs("transactional")
                .isEqualTo(expected);
        return this;
    }

    /**
     * Verifies that the batch has the expected partition leader epoch.
     *
     * @param expected the expected partition leader epoch
     * @return this assertion
     */
    public RecordBatchAssert hasPartitionLeaderEpoch(int expected) {
        isNotNull();
        Assertions.assertThat(actual.partitionLeaderEpoch())
                .describedAs("partitionLeaderEpoch")
                .isEqualTo(expected);
        return this;
    }

    /**
     * Verifies that the batch has the expected delete horizon.
     *
     * @param expected the expected delete horizon in milliseconds
     * @return this assertion
     */
    public RecordBatchAssert hasDeleteHorizonMs(OptionalLong expected) {
        isNotNull();
        Assertions.assertThat(actual.deleteHorizonMs())
                .describedAs("deleteHorizonMs")
                .isNotNull()
                .isEqualTo(expected);
        return this;
    }

    /**
     * Verifies that the batch has the expected last offset.
     *
     * @param expected the expected last offset
     * @return this assertion
     */
    public RecordBatchAssert hasLastOffset(long expected) {
        isNotNull();
        Assertions.assertThat(actual.lastOffset())
                .describedAs("lastOffset")
                .isEqualTo(expected);
        return this;
    }

    /**
     * Verifies that the batch has the same metadata as the given batch.
     *
     * @param batch the batch with the expected metadata
     * @return this assertion
     */
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

    /**
     * Verifies that the batch has the expected last sequence.
     *
     * @param expected the expected last sequence
     * @return this assertion
     */
    public RecordBatchAssert hasLastSequence(int expected) {
        isNotNull();
        Assertions.assertThat(actual.lastSequence())
                .describedAs("lastSequence")
                .isEqualTo(expected);
        return this;
    }

    /**
     * Verifies that the batch has the expected producer epoch.
     *
     * @param expected the expected producer epoch
     * @return this assertion
     */
    public RecordBatchAssert hasProducerEpoch(short expected) {
        isNotNull();
        Assertions.assertThat(actual.producerEpoch())
                .describedAs("producerEpoch")
                .isEqualTo(expected);
        return this;
    }

    /**
     * Verifies that the batch has the expected producer id.
     *
     * @param expected the expected producer id
     * @return this assertion
     */
    public RecordBatchAssert hasProducerId(long expected) {
        isNotNull();
        Assertions.assertThat(actual.producerId())
                .describedAs("producerId")
                .isEqualTo(expected);
        return this;
    }

    /**
     * Verifies that the batch has the expected max timestamp.
     *
     * @param expected the expected max timestamp
     * @return this assertion
     */
    public RecordBatchAssert hasMaxTimestamp(long expected) {
        isNotNull();
        Assertions.assertThat(actual.maxTimestamp())
                .describedAs("maxTimestamp")
                .isEqualTo(expected);
        return this;
    }

    /**
     * Verifies that the batch has the expected timestamp type.
     *
     * @param expected the expected timestamp type
     * @return this assertion
     */
    public RecordBatchAssert hasTimestampType(TimestampType expected) {
        isNotNull();
        Assertions.assertThat(actual.timestampType())
                .describedAs("timestampType")
                .isEqualTo(expected);
        return this;
    }

    private IterableAssert<Record> recordIterable() {
        isNotNull();
        return IterableAssert.assertThatIterable(actual)
                .describedAs("records");
    }

    /**
     * Verifies that the batch has records and creates an assertion for the first one.
     *
     * @return the first record assertion
     */
    public RecordAssert firstRecord() {
        isNotNull();
        isNotEmpty();
        return recordIterable()
                .first(new InstanceOfAssertFactory<>(Record.class, RecordAssert::assertThat))
                .describedAs("first record");
    }

    /**
     * Verifies that the batch has records and creates an assertion for the last one.
     *
     * @return the last record assertion
     */
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

    /**
     * Returns an iterable of assertions, one for each record in the batch.
     *
     * @return the record assertions
     */
    public Iterable<RecordAssert> records() {
        isNotNull();
        return () -> new Iterator<>() {
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
    }
}
