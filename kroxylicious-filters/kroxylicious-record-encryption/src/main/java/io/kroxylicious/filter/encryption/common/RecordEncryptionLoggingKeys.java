/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.common;

/**
 * Common keys for structured logging in kroxylicious-record-encryption.
 */
public class RecordEncryptionLoggingKeys {

    private RecordEncryptionLoggingKeys() {
    }

    /**
     * Attempt number for retry operations.
     */
    public static final String ATTEMPT = "attempt";

    /**
     * Cipher algorithm name.
     */
    public static final String CIPHER_ALGORITHM = "cipherAlgorithm";

    /**
     * Cipher specification for encryption.
     */
    public static final String CIPHER_SPEC = "cipherSpec";

    /**
     * Data Encryption Key (DEK) information.
     */
    public static final String DEK = "dek";

    /**
     * Size of the encryption buffer.
     */
    public static final String ENCRYPTION_BUFFER = "encryptionBuffer";

    /**
     * Error message or exception description.
     */
    public static final String ERROR = "error";

    /**
     * Failures encountered during processing.
     */
    public static final String FAILURES = "failures";

    /**
     * Key Encryption Key (KEK) identifier.
     */
    public static final String KEK_ID = "kekId";

    /**
     * KMS cache configuration.
     */
    public static final String KMS_CACHE_CONFIG = "kmsCacheConfig";

    /**
     * KMS operation being performed.
     */
    public static final String KMS_OPERATION = "kmsOperation";

    /**
     * Partition number.
     */
    public static final String PARTITION = "partition";

    /**
     * Cryptographic provider name.
     */
    public static final String PROVIDER = "provider";

    /**
     * Kafka topic name.
     */
    public static final String TOPIC_NAME = "topicName";
}
