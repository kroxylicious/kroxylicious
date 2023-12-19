/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

// TODO singleton? The generics make that annoying, but I don't think we need them...
public class PlaintextMessageEncryptionScheme<K> implements MessageEncryptionScheme<K> {

    @Override
    public CompletionStage<ProduceRequestData> encrypt(RequestHeaderData recordHeaders, ProduceRequestData requestData,
                                                       IntFunction<ByteBufferOutputStream> bufferFactory) {
        return CompletableFuture.completedStage(requestData);
    }

    @Override
    public CompletionStage<FetchResponseData> decrypt(ResponseHeaderData header, FetchResponseData response) {
        return CompletableFuture.completedStage(response);
    }

    // TODO revisit equals and hashcode.
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode();
    }
}
