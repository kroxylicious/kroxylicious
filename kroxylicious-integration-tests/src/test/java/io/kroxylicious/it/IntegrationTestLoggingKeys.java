/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it;

/**
 * Common keys for structured logging in kroxylicious-integration-tests.
 */
public class IntegrationTestLoggingKeys {

    private IntegrationTestLoggingKeys() {
    }

    /**
     * The authorization id.
     */
    public static final String AUTHORIZATION_ID = "authorizationId";

    /**
     * The session id.
     */
    public static final String SESSION_ID = "sessionId";

}
