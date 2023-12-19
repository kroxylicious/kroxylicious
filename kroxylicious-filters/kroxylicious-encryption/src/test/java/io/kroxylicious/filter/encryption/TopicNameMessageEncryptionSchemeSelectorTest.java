/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ProduceRequestData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

class TopicNameMessageEncryptionSchemeSelectorTest {

    public static final String ENCRYPTED_TOPIC_NAME = "encrypted";
    public static final String PLAINTEXT_TOPIC_NAME = "unencrypted";

    private TopicNameEncryptionSchemeSelector<String> encryptionSchemeSelector;

    @BeforeEach
    void setUp() {
        encryptionSchemeSelector = new TopicNameEncryptionSchemeSelector<>(Set.of(ENCRYPTED_TOPIC_NAME));
    }

    @Test
    void shouldDefaultToUnencryptedScheme() {
        // Given
        final ProduceRequestData produceRequestData = produceRequestForTopic(PLAINTEXT_TOPIC_NAME);

        // When
        final CompletionStage<MessageEncryptionScheme<String>> actualEncryptionScheme = encryptionSchemeSelector.selectFor(null, produceRequestData);

        // Then
        assertThat(actualEncryptionScheme).isCompletedWithValue(new PlaintextMessageEncryptionScheme<>());
    }

    @Test
    void shouldReturnKekPerTopicScheme() {
        final ProduceRequestData produceRequestData = produceRequestForTopic(ENCRYPTED_TOPIC_NAME);

        // When
        final CompletionStage<MessageEncryptionScheme<String>> actualEncryptionScheme = encryptionSchemeSelector.selectFor(null, produceRequestData);

        // Then
        assertThat(actualEncryptionScheme).isCompletedWithValueMatching(encryptionScheme -> encryptionScheme instanceof KekPerTopicMessageEncryptionScheme<String>);
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