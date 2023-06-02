/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.topic;

import org.apache.kafka.common.message.ProduceRequestData;

/**
 * Validates ${@link org.apache.kafka.common.message.ProduceRequestData.TopicProduceData}
 */
public interface TopicValidator {
    /**
     * Validate topic produce data, returning details about which partitions/records were
     * invalid
     * @param request the request
     * @return result describing whether any partitions were invalid, and details of any invalid partitions/records
     */
    TopicValidationResult validateTopicData(ProduceRequestData.TopicProduceData request);
}
