/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.oauthbearer;

/**
 * Common keys for structured logging in kroxylicious-oauthbearer-validation.
 */
public class OAuthBearerValidationLoggingKeys {

    private OAuthBearerValidationLoggingKeys() {
    }

    /**
     * Error message or exception description.
     */
    public static final String ERROR = "error";

    /**
     * The SASL mechanism name.
     */
    public static final String MECHANISM = "mechanism";

    /**
     * Outcome of the validation attempt.
     */
    public static final String OUTCOME = "outcome";
}
