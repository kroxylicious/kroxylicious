/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.model;

/**
 * Constants used to interact with the Fortanix DSM REST API.
 */
public class Constants {

    /**
     * AES algorithm name.
     */
    public static final String AES = "AES";
    /**
     * CBC cipher mode.
     */
    public static final String BATCH_ENCRYPT_CIPHER_MODE = "CBC";
    /**
     * Export key operations.
     */
    public static final String EXPORT_KEY_OPS = "EXPORT";

    private Constants() {
        // unused
    }
}
