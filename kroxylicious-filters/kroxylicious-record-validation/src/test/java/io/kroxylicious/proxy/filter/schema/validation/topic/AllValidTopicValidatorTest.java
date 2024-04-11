/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.topic;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ProduceRequestData;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AllValidTopicValidatorTest {

    public static final String TOPIC_NAME = "topicName";

    @Test
    void allValid() {
        AllValidTopicValidator validator = new AllValidTopicValidator();
        ProduceRequestData.TopicProduceData data = new ProduceRequestData.TopicProduceData();
        data.setName(TOPIC_NAME);
        CompletionStage<TopicValidationResult> result = validator.validateTopicData(data);
        assertThat(result).succeedsWithin(Duration.ZERO).isEqualTo(new AllValidTopicValidationResult(TOPIC_NAME));
    }

}