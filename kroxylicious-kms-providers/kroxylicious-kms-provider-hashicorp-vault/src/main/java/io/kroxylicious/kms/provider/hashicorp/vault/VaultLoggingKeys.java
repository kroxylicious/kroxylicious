/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

/**
 * Common keys for structured logging in kroxylicious-kms-provider-hashicorp-vault.
 */
public class VaultLoggingKeys {

    private VaultLoggingKeys() {
    }
    
    /**
     * Error.
     */
    public static final String ERROR = "error";
    
    /**
     * Kek ref.
     */
    public static final String KEK_REF = "kekRef";
    
    /**
     * Request uri.
     */
    public static final String REQUEST_URI = "requestUri";
    
    /**
     * Response body.
     */
    public static final String RESPONSE_BODY = "responseBody";
    
    /**
     * Status code.
     */
    public static final String STATUS_CODE = "statusCode";
}
