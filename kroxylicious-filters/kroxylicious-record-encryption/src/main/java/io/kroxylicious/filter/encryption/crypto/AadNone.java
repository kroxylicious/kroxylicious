/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import java.nio.ByteBuffer;

import io.kroxylicious.kafka.common.record.internal.RecordBatch;

import io.kroxylicious.filter.encryption.config.AadSpec;
import io.kroxylicious.kafka.common.utils.ByteUtils;

/**
 * An {@link Aad} which computes an empty AAD, meaning no additional data is
 * authenticated by the cipher.
 */
public class AadNone implements Aad {

    /** The singleton instance of this AAD. */
    public static final AadNone INSTANCE = new AadNone();

    private AadNone() {
    }

    @Override
    public ByteBuffer computeAad(
                                 String topicName,
                                 int partitionId,
                                 RecordBatch batch) {
        return ByteUtils.EMPTY_BUF;
    }

    @Override
    public byte serializedId() {
        return 0;
    }

    @Override
    public AadSpec name() {
        return AadSpec.NONE;
    }
}
