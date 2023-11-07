/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.topic;

import org.apache.kafka.common.message.ProduceRequestData;

class AllValidTopicValidator implements TopicValidator {
    @Override
    public TopicValidationResult validateTopicData(ProduceRequestData.TopicProduceData request) {
        return new AllValidTopicValidationResult(request.name());
    }
}
