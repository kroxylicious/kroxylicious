/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import java.nio.ByteBuffer;

import org.apache.kafka.common.record.RecordBatch;

import io.kroxylicious.filter.encryption.common.PersistedIdentifiable;
import io.kroxylicious.filter.encryption.config.AadSpec;

public interface Aad extends PersistedIdentifiable<AadSpec> {
    ByteBuffer computeAad(String topicName, int partitionId, RecordBatch batch);
}
