/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import org.apache.kafka.common.message.ProduceRequestData;
import org.assertj.core.api.Condition;

public class ProduceRequestDataCondition extends Condition<ProduceRequestData> {

    private final String topicName;

    public ProduceRequestDataCondition(String topicName) {
        this.topicName = topicName;
    }

    public static Condition<ProduceRequestData> hasRecordsForTopic(String topicName) {
        return new ProduceRequestDataCondition(topicName);
    }

    @Override
    public boolean matches(ProduceRequestData value) {
        return value.topicData().stream().anyMatch(topicProduceData -> topicProduceData.name().equals(topicName));
    }
}