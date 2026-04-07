/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

/**
 * Common keys for structured logging in kroxylicious-kms-provider-hashicorp-vault-test-support.
 */
public class VaultTestSupportLoggingKeys {

    private VaultTestSupportLoggingKeys() {
    }

    /**
     * The body.
     */
    public static final String BODY = "body";

    /**
     * The status code.
     */
    public static final String STATUS_CODE = "statusCode";

    /**
     * Url.
     */
    public static final String URL = "url";

}
