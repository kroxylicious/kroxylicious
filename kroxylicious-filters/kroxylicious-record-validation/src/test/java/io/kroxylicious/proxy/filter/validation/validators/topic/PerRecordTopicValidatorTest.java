/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.topic;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.record.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.validation.validators.Result;
import io.kroxylicious.proxy.filter.validation.validators.record.RecordValidator;
import io.kroxylicious.test.record.RecordTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.stream;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PerRecordTopicValidatorTest {

    @Mock
    private RecordValidator validator;

    @Test
    void singleValidRecord() {
        var tpd = createTopicProduceDataWithOnePartition(RecordTestUtils.record("good"));
        var topicValidator = new PerRecordTopicValidator(validator);

        when(validator.validate(any(Record.class))).thenReturn(Result.VALID_RESULT_STAGE);
        var result = topicValidator.validateTopicData(tpd);
        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, TopicValidationResult::isAnyPartitionInvalid)
                .extracting(TopicValidationResult::invalidPartitions, stream(PartitionValidationResult.class))
                .isEmpty();

        verify(this.validator, times(1)).validate(any(Record.class));
    }

    @Test
    void singleInvalidRecord() {
        var tpd = createTopicProduceDataWithOnePartition(RecordTestUtils.record("bad"));
        var topicValidator = new PerRecordTopicValidator(validator);

        when(validator.validate(any(Record.class))).thenReturn(CompletableFuture.completedStage(new Result(false, "my bad record")));
        var result = topicValidator.validateTopicData(tpd);
        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, TopicValidationResult::isAnyPartitionInvalid);

        var invalidPartitions = result.toCompletableFuture().join().invalidPartitions();
        assertThat(invalidPartitions)
                .singleElement()
                .extracting(PartitionValidationResult::recordValidationFailures, list(RecordValidationFailure.class))
                .singleElement()
                .extracting(RecordValidationFailure::errorMessage, STRING)
                .contains("my bad record");

        verify(this.validator, times(1)).validate(any(Record.class));
    }

    @Test
    void mixedResultWithinSinglePartition() {
        var good = RecordTestUtils.record(0, null, "good");
        var bad = RecordTestUtils.record(1, null, "bad");
        var tpd = createTopicProduceDataWithOnePartition(good, bad);
        var topicValidator = new PerRecordTopicValidator(validator);

        when(validator.validate(good)).thenReturn(Result.VALID_RESULT_STAGE);
        when(validator.validate(bad)).thenReturn(CompletableFuture.completedStage(new Result(false, "my bad record")));
        var result = topicValidator.validateTopicData(tpd);

        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .extracting(TopicValidationResult::invalidPartitions, stream(PartitionValidationResult.class))
                .singleElement()
                .extracting(PartitionValidationResult::recordValidationFailures, list(RecordValidationFailure.class))
                .singleElement()
                .returns("my bad record", RecordValidationFailure::errorMessage)
                .returns(1, RecordValidationFailure::invalidIndex);
    }

    @Test
    void mixedResultAcrossTwoPartitions() {
        var good = RecordTestUtils.record("good");
        var bad = RecordTestUtils.record(0, null, "bad");
        var ugly = RecordTestUtils.record(1, null, "ugly");

        var tpd = new TopicProduceData();
        tpd.partitionData().add(makePartitionProduceData(0, good));
        tpd.partitionData().add(makePartitionProduceData(1, bad, ugly));

        var topicValidator = new PerRecordTopicValidator(validator);

        when(validator.validate(good)).thenReturn(Result.VALID_RESULT_STAGE);
        when(validator.validate(bad)).thenReturn(CompletableFuture.completedStage(new Result(false, "my bad record")));
        when(validator.validate(ugly)).thenReturn(CompletableFuture.completedStage(new Result(false, "my ugly record")));

        var result = topicValidator.validateTopicData(tpd);

        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, TopicValidationResult::isAnyPartitionInvalid);

        var invalidPartitions = result.toCompletableFuture().join().invalidPartitions();
        assertThat(invalidPartitions)
                .singleElement()
                .returns(1, PartitionValidationResult::index)
                .extracting(PartitionValidationResult::recordValidationFailures, list(RecordValidationFailure.class))
                .hasSize(2);
    }

    @Test
    void mixedResultWithAllPartitionsHavingSomeBadData() {
        var good = RecordTestUtils.record(0, null, "good");
        var bad = RecordTestUtils.record(1, null, "bad");
        var ugly = RecordTestUtils.record(0, null, "ugly");

        var tpd = new TopicProduceData();
        tpd.partitionData().add(makePartitionProduceData(0, good, bad));
        tpd.partitionData().add(makePartitionProduceData(1, ugly));

        var topicValidator = new PerRecordTopicValidator(validator);

        when(validator.validate(good)).thenReturn(Result.VALID_RESULT_STAGE);
        when(validator.validate(bad)).thenReturn(CompletableFuture.completedStage(new Result(false, "my bad record")));
        when(validator.validate(ugly)).thenReturn(CompletableFuture.completedStage(new Result(false, "my ugly record")));

        var result = topicValidator.validateTopicData(tpd);

        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, TopicValidationResult::isAnyPartitionInvalid);

        var invalidPartitions = result.toCompletableFuture().join().invalidPartitions().toList();
        assertThat(invalidPartitions).hasSize(2);

        assertThat(invalidPartitions.get(0))
                .returns(0, PartitionValidationResult::index)
                .extracting(PartitionValidationResult::recordValidationFailures, list(RecordValidationFailure.class))
                .hasSize(1);

        assertThat(invalidPartitions.get(1))
                .returns(1, PartitionValidationResult::index)
                .extracting(PartitionValidationResult::recordValidationFailures, list(RecordValidationFailure.class))
                .hasSize(1);
    }

    private static TopicProduceData createTopicProduceDataWithOnePartition(Record... records) {
        var tpd = new TopicProduceData();
        tpd.partitionData().add(makePartitionProduceData(0, records));
        return tpd;
    }

    private static PartitionProduceData makePartitionProduceData(int index, Record... records) {
        var partitionProduceData = new PartitionProduceData();
        partitionProduceData.setIndex(index).setRecords(RecordTestUtils.memoryRecords(Arrays.stream(records).toList()));
        return partitionProduceData;
    }
}
