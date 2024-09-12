/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.decrypt;

import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import edu.umd.cs.findbugs.annotations.NonNull;

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
            @NonNull
            String topicName,
            int partition,
            @NonNull
            MemoryRecords records,
            @NonNull
            IntFunction<ByteBufferOutputStream> bufferAllocator
    );
}
