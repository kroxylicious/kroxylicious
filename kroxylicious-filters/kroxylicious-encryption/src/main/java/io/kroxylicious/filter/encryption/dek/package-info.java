/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * {@linkplain io.kroxylicious.filter.encryption.dek.DekManager DEK management} that encapsulates a {@link io.kroxylicious.kms.service.Kms} to provide
 * {@linkplain io.kroxylicious.filter.encryption.dek.DataEncryptionKey managed access} to
 * data {@linkplain io.kroxylicious.filter.encryption.dek.DataEncryptionKey.Encryptor encryption} and
 * {@linkplain io.kroxylicious.filter.encryption.dek.DataEncryptionKey.Decryptor decryption} operations
 * without exposing keys to the rest of the application.
 */
package io.kroxylicious.filter.encryption.dek;