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
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

public record PlaintextEncryptionScheme<K>() implements RecordEncryptionScheme<K>, MessageEncryptionScheme<K> {

    @Override
    public CompletionStage<Record> encrypt(Record plainTextRecord, RequestHeaderData recordHeaders, ProduceRequestData produceRequestData) {
        return null;
    }

    @Override
    public CompletionStage<Record> decrypt(Record encipheredRecord, ResponseHeaderData responseHeaderData, FetchResponseData fetchResponseData) {
        return null;
    }

    @Override
    public CompletionStage<ProduceRequestData> encrypt(RequestHeaderData recordHeaders, ProduceRequestData requestData,
                                                       IntFunction<ByteBufferOutputStream> bufferFactory) {
        return null;
    }

    @Override
    public CompletionStage<FetchResponseData> decrypt(ResponseHeaderData header, FetchResponseData response) {
        return null;
    }
}
