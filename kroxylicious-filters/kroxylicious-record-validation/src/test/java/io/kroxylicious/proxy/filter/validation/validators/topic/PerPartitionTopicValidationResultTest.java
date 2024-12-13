/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.topic;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.STREAM;

class PerPartitionTopicValidationResultTest {

    private static final RecordValidationFailure INVALID = new RecordValidationFailure(0, "invalid");

    @Test
    void empty() {
        var result = new PerPartitionTopicValidationResult("foo", Map.of());

        assertThat(result)
                .returns(false, PerPartitionTopicValidationResult::isAnyPartitionInvalid);
        assertThat(result)
                .extracting(PerPartitionTopicValidationResult::invalidPartitions, STREAM)
                .isEmpty();
    }

    @Test
    void partitionWithAllInvalidResults() {
        var result = new PerPartitionTopicValidationResult("foo",
                Map.of(1, new PartitionValidationResult(2, List.of(INVALID))));

        assertThat(result)
                .returns(true, PerPartitionTopicValidationResult::isAnyPartitionInvalid);
    }

    @Test
    void partitionWithOneInvalidResults() {
        var result = new PerPartitionTopicValidationResult("foo",
                Map.of(0, new PartitionValidationResult(0, List.of()),
                        1, new PartitionValidationResult(1, List.of(INVALID))));

        assertThat(result)
                .returns(true, PerPartitionTopicValidationResult::isAnyPartitionInvalid);
    }

    @Test
    void byPartitionId() {
        var result = new PerPartitionTopicValidationResult("foo",
                Map.of(0, new PartitionValidationResult(10, List.of(INVALID)),
                        1, new PartitionValidationResult(100, List.of())));

        assertThat(result.getPartitionResult(0))
                .extracting(PartitionValidationResult::index)
                .isEqualTo(10);

        assertThat(result.getPartitionResult(1))
                .extracting(PartitionValidationResult::index)
                .isEqualTo(100);
    }

    @Test
    void invalidPartitions() {
        var result = new PerPartitionTopicValidationResult("foo",
                Map.of(0, new PartitionValidationResult(0, List.of()),
                        1, new PartitionValidationResult(100, List.of(INVALID))));

        assertThat(result.invalidPartitions())
                .singleElement()
                .extracting(PartitionValidationResult::index)
                .isEqualTo(100);
    }

    @Test
    void topicName() {
        var result = new PerPartitionTopicValidationResult("foo", Map.of());

        assertThat(result.topicName())
                .isEqualTo("foo");
    }

}
