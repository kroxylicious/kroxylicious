/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import java.nio.ByteBuffer;

import io.kroxylicious.kafka.common.record.internal.RecordBatch;

import io.kroxylicious.filter.encryption.common.PersistedIdentifiable;
import io.kroxylicious.filter.encryption.config.AadSpec;

/**
 * Abstraction for constructing the AAD passed to an AEAD cipher.
 */
public interface Aad extends PersistedIdentifiable<AadSpec> {
    /**
     * Computes the AAD for the given batch of records.
     * @param topicName the name of the topic to which the batch is being produced, or from which it is being fetched.
     * @param partitionId the index of the partition to which the batch is being produced, or from which it is being fetched.
     * @param batch the batch of records.
     * @return a buffer containing the AAD.
     */
    ByteBuffer computeAad(String topicName, int partitionId, RecordBatch batch);
}
