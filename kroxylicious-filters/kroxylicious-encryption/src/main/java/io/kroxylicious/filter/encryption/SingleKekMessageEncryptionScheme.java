/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

/**
 * Describes how a record should be encrypted
 * @param kekId The KEK identifier to be used. Not null.
 * @param recordFields The fields of the record that should be encrypted with the given KEK. Neither null nor empty.
 * @param <K> The type of KEK identifier.
 */
public record SingleKekMessageEncryptionScheme<K>(
                                                  K kekId,
                                                  Set<RecordField> recordFields)
        implements MessageEncryptionScheme<K> {
    public SingleKekMessageEncryptionScheme {
        Objects.requireNonNull(kekId);
        if (Objects.requireNonNull(recordFields).isEmpty()) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public CompletionStage<ProduceRequestData> encrypt(RequestHeaderData recordHeaders, ProduceRequestData requestData,
                                                       IntFunction<ByteBufferOutputStream> bufferFactory) {
        return CompletableFuture.completedStage(null);
    }

    @Override
    public CompletionStage<FetchResponseData> decrypt(ResponseHeaderData header, FetchResponseData response) {
        return CompletableFuture.completedStage(null);
    }
}
