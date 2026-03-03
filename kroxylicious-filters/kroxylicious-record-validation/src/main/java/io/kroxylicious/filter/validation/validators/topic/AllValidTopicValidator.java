/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation.validators.topic;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ProduceRequestData;

class AllValidTopicValidator implements TopicValidator {
    @Override
    public CompletionStage<TopicValidationResult> validateTopicData(ProduceRequestData.TopicProduceData request) {
        return CompletableFuture.completedFuture(new AllValidTopicValidationResult(request.name()));
    }
}
