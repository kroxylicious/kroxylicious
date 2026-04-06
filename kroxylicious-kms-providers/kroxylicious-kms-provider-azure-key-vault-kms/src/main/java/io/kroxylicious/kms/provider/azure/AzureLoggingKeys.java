/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure;

/**
 * Common keys for structured logging in kroxylicious-kms-provider-azure-key-vault-kms.
 */
public class AzureLoggingKeys {

    private AzureLoggingKeys() {
    }

    /**
     * Alias.
     */
    public static final String ALIAS = "alias";

    /**
     * Endpoint.
     */
    public static final String ENDPOINT = "endpoint";

    /**
     * Error.
     */
    public static final String ERROR = "error";

    /**
     * From state.
     */
    public static final String FROM_STATE = "fromState";

    /**
     * Key name.
     */
    public static final String KEY_NAME = "keyName";

    /**
     * Key version.
     */
    public static final String KEY_VERSION = "keyVersion";

    /**
     * Response.
     */
    public static final String RESPONSE = "response";

    /**
     * Scheme.
     */
    public static final String SCHEME = "scheme";

    /**
     * Status code.
     */
    public static final String STATUS_CODE = "statusCode";

    /**
     * To state.
     */
    public static final String TO_STATE = "toState";
}
