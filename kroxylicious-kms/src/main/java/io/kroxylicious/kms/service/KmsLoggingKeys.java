/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

/**
 * Common keys for structured logging in kroxylicious-kms.
 */
public class KmsLoggingKeys {

    private KmsLoggingKeys() {
    }

    /**
     * Error message or exception description.
     */
    public static final String ERROR = "error";

    /**
     * Fully-qualified class name of the cryptographic key.
     */
    public static final String KEY_CLASS = "keyClass";
}
