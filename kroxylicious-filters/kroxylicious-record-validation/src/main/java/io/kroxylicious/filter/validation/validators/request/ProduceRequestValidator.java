/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation.validators.request;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ProduceRequestData;

/**
 * Validate that all Records in a Produce Request are valid and return a result
 * describing which records were invalid.
 */
public interface ProduceRequestValidator {

    /**
     * Validate a request
     * @param namedTopicProduceDataList the named topic request data from a single request
     * @return result describing a validation outcome for all topic partitions and details of records that failed validation
     */
    CompletionStage<ProduceRequestValidationResult> validateRequest(List<NamedTopicProduceData> namedTopicProduceDataList);

    record NamedTopicProduceData(String topicName, ProduceRequestData.TopicProduceData data) {
        public NamedTopicProduceData {
            Objects.requireNonNull(topicName);
            Objects.requireNonNull(data);
            if (topicName.isEmpty()) {
                throw new IllegalStateException("topic name is empty");
            }
        }
    }
}
