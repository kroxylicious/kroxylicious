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

record DecryptState(@NonNull Record kafkaRecord, @Nullable EncryptionVersion decryptionVersion,
                    @Nullable AesGcmEncryptor encryptor) {}
