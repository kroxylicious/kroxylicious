/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ProduceRequestData;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class PlaintextEncryptionSchemeTest {

    @Test
    void shouldReturnOriginalRequest() {
        // Given
        final ProduceRequestData expected = produceRequestForTopic("wibble");

        // When
        final CompletionStage<ProduceRequestData> actual = new PlaintextEncryptionScheme<String>().encrypt(null, expected, null);

        // Then
        assertThat(actual).isCompleted().isCompletedWithValueMatching(produceRequestData -> System.identityHashCode(expected) == System.identityHashCode(
                produceRequestData));
    }

    @NonNull
    private static ProduceRequestData produceRequestForTopic(String encryptedTopicName) {
        final ProduceRequestData produceRequestData = new ProduceRequestData();
        final ProduceRequestData.TopicProduceDataCollection topicProduceData = new ProduceRequestData.TopicProduceDataCollection();
        final ProduceRequestData.TopicProduceData produceData = new ProduceRequestData.TopicProduceData();
        produceData.setName(encryptedTopicName);
        topicProduceData.add(produceData);
        produceRequestData.setTopicData(topicProduceData);
        return produceRequestData;
    }
}
