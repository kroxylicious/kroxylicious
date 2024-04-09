/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import java.nio.ByteBuffer;

import org.apache.kafka.common.record.RecordBatch;

public interface Aad {
    ByteBuffer computeAad(String topicName, int partitionId, RecordBatch batch);
}
