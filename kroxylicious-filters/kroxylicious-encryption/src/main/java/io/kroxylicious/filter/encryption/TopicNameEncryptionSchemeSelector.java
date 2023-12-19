/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;

public class TopicNameEncryptionSchemeSelector<K> implements EncryptionSchemeSelector<K> {
    private final Set<String> encryptedTopicNames;
    private final KeyManager<K> keyManager;

    public TopicNameEncryptionSchemeSelector(Set<String> encryptedTopicNames) {
        this.encryptedTopicNames = encryptedTopicNames;
        keyManager = null;
    }

    @Override
    public CompletionStage<MessageEncryptionScheme<K>> selectFor(RequestHeaderData recordHeaders, ProduceRequestData produceRequestData) {
        return CompletableFuture.completedStage(new PerTopicMessageEncryptionScheme<K>(Map.of(), new PlaintextEncryptionScheme<K>()));
    }

    @Override
    public CompletionStage<RecordEncryptionScheme<K>> selectFor(RequestHeaderData recordHeaders, ProduceRequestData.TopicProduceData topicProduceData, String topicName) {
        return null;
    }
}
