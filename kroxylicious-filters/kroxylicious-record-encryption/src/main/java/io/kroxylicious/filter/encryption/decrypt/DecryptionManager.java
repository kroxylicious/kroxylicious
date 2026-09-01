/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.decrypt;

import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;

import io.kroxylicious.kafka.common.record.internal.MemoryRecords;

import edu.umd.cs.findbugs.annotations.NonNull;

import io.kroxylicious.kafka.common.utils.ByteBufferOutputStream;

/**
 * A manager of (data) encryption keys supporting decryption operations,
 * encapsulating access to the data encryption keys.
 */
public interface DecryptionManager {
    /**
     * Asynchronously decrypt the given {@code records}, returning a MemoryRecords object which will contain all records transformed to their decrypted form (if required)
     * @param topicName The topic name
     * @param partition The partition index
     * @param records The records
     * @param bufferAllocator Allocator of ByteBufferOutputStream
     * @return A completion stage that completes with the output MemoryRecords when all the records have been processed and transformed.
     */
    @NonNull
    CompletionStage<MemoryRecords> decrypt(
                                           @NonNull String topicName,
                                           int partition,
                                           @NonNull MemoryRecords records,
                                           @NonNull IntFunction<ByteBufferOutputStream> bufferAllocator);
}
