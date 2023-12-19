/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

public interface MessageEncryptionScheme<K> {
    CompletionStage<ProduceRequestData> encrypt(RequestHeaderData recordHeaders, ProduceRequestData requestData, IntFunction<ByteBufferOutputStream> bufferFactory);

    CompletionStage<FetchResponseData> decrypt(ResponseHeaderData header, FetchResponseData response);
}
