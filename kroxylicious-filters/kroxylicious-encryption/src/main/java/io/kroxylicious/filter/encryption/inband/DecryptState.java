/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import org.apache.kafka.common.record.Record;

import io.kroxylicious.filter.encryption.EncryptionVersion;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Helper class to group together some state for decryption.
 * Either both, or neither, of the given {@code decryptionVersion} and {@code encryptor} should be null.
 * @param kafkaRecord The record
 * @param decryptionVersion The version
 * @param encryptor The encryptor
 */
record DecryptState(@NonNull Record kafkaRecord, @Nullable EncryptionVersion decryptionVersion,
                    @Nullable AesGcmEncryptor encryptor) {

    DecryptState {
        if (decryptionVersion == null ^ encryptor == null) {
            throw new IllegalArgumentException();
        }
    }
}
