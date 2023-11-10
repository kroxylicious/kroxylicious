/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.record.Record;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A manager of (data) encryption keys supporting encryption and decryption operations,
 * encapsulating access to the data encryption keys.
 * @param <K> The type of KEK id.
 */
public interface KeyManager<K> {

    /**
     * Asynchronously encrypt the given {@code recordRequests} using the current DEK for the given KEK, calling the given receiver for each encrypted record
     * @param encryptionScheme The encryption scheme.
     * @param records The requests to be encrypted.
     * @param receiver The receiver of the encrypted buffers.
     * @return A completion stage that completes when all the records in the given {@code recordRequests} have been processed.
     */
    @NonNull
    CompletionStage<Void> encrypt(@NonNull EncryptionScheme<K> encryptionScheme,
                                  @NonNull List<? extends Record> records,
                                  @NonNull Receiver receiver);

    /**
     * Asynchronously decrypt the given {@code kafkaRecords} (if they were, in fact, encrypted), calling the given {@code receiver} with the plaintext
     * @param records The records
     * @param receiver The receiver of the plaintext buffers
     * @return A completion stage that completes when the record has been processed.
     */
    @NonNull
    CompletionStage<Void> decrypt(@NonNull List<? extends Record> records,
                                  @NonNull Receiver receiver);
}
