/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;

public class TopicNameEncryptionSchemeSelector<K> implements EncryptionSchemeSelector<K> {
    @Override
    public CompletionStage<EncryptionScheme<K>> selectFor(RequestHeaderData recordHeaders, ProduceRequestData produceRequestData) {
        return null;
    }
}
