/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

/**
 * Common keys for structured logging in kroxylicious-kms-provider-fortanix-dsm.
 */
public class FortanixDsmLoggingKeys {

    private FortanixDsmLoggingKeys() {
    }

    /**
     * Delay ms.
     */
    public static final String DELAY_MS = "delayMs";

    /**
     * Error.
     */
    public static final String ERROR = "error";

    /**
     * Expiration.
     */
    public static final String EXPIRATION = "expiration";

    /**
     * Status code.
     */
    public static final String STATUS_CODE = "statusCode";
}
