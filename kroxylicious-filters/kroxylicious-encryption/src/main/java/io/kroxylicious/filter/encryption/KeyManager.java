/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A manager of (data) encryption keys supporting encryption and decryption operations,
 * encapsulating access to the data encryption keys.
 * @param <K> The type of KEK id.
 */
public interface KeyManager<K> {

    /**
     * Asynchronously encrypt the given {@code records} using the current DEK for the given KEK returning a MemoryRecords object which will contain all records
     * transformed according to the {@code encryptionScheme}.
     * @param encryptionScheme The encryption scheme.
     * @param records The MemoryRecords to be encrypted.
     * @param bufferAllocator Allocator of ByteBufferOutputStream
     * @return A completion stage that completes with the output MemoryRecords when all the records have been processed and transformed.
     */
    @NonNull
    CompletionStage<MemoryRecords> encrypt(@NonNull String topicName,
                                           int partition,
                                           @NonNull EncryptionScheme<K> encryptionScheme,
                                           @NonNull MemoryRecords records,
                                           @NonNull IntFunction<ByteBufferOutputStream> bufferAllocator);

    /**
     * Asynchronously decrypt the given {@code kafkaRecords} (if they were, in fact, encrypted), calling the given {@code receiver} with the plaintext
     * @param topicName The topic name
     * @param partition The partition index
     * @param records The records
     * @param receiver The receiver of the plaintext buffers. The receiver is guaranteed to receive the decrypted buffers sequentially, in the same order as {@code records}, with no possibility of parallel invocation.
     * @return A completion stage that completes when all the records have been processed.
     */
    @NonNull
    CompletionStage<Void> decrypt(String topicName, int partition, @NonNull List<? extends Record> records,
                                  @NonNull Receiver receiver);
}
