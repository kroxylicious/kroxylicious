/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import io.kroxylicious.filter.encryption.config.RecordEncryptionConfig;
import io.kroxylicious.filter.encryption.decrypt.DecryptionDekCache;
import io.kroxylicious.filter.encryption.dek.DekManager;
import io.kroxylicious.filter.encryption.encrypt.EncryptionDekCache;
import io.kroxylicious.kms.service.Kms;

/**
 * Things which are shared between instances of the filter.
 * Because they're shared between filter instances, the things shared here must be thread-safe.
 *
 * @param kms KMS
 * @param kmsServiceCloser KMS closer
 * @param configuration configuration
 * @param dekManager DEK manager
 * @param encryptionDekCache Encryption DEK Cache
 * @param decryptionDekCache Decryption DEK Cache
 *
 * @param <K> The type of KEK id.
 * @param <E> The type of the encrypted DEK.
 */
record SharedEncryptionContext<K, E>(Kms<K, E> kms,
                                     Runnable kmsServiceCloser,
                                     RecordEncryptionConfig configuration,
                                     DekManager<K, E> dekManager,
                                     EncryptionDekCache<K, E> encryptionDekCache,
                                     DecryptionDekCache<K, E> decryptionDekCache) {}
