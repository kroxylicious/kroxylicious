/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;

public interface EncryptionSchemeSelector<K> {

    CompletionStage<MessageEncryptionScheme<K>> selectFor(RequestHeaderData recordHeaders, ProduceRequestData produceRequestData);

    CompletionStage<RecordEncryptionScheme<K>> selectFor(RequestHeaderData recordHeaders, ProduceRequestData.TopicProduceData topicProduceData, String topicName);
}
