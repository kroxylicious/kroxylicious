/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins;

import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * Extracts non-zero topic UUIDs from Kafka request messages.
 */
final class TopicIdExtractor {

    private TopicIdExtractor() {
    }

    static Set<Uuid> extractTopicIds(ApiKeys apiKey, ApiMessage request) {
        Set<Uuid> ids = new HashSet<>();
        if (request instanceof ProduceRequestData pr) {
            for (var td : pr.topicData()) {
                if (!Uuid.ZERO_UUID.equals(td.topicId())) {
                    ids.add(td.topicId());
                }
            }
        }
        else if (request instanceof FetchRequestData fr) {
            for (var topic : fr.topics()) {
                if (!Uuid.ZERO_UUID.equals(topic.topicId())) {
                    ids.add(topic.topicId());
                }
            }
        }
        return ids;
    }
}
