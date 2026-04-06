/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.inspection;

/**
 * Common keys for structured logging in kroxylicious-sasl-inspection.
 */
public class SaslInspectionLoggingKeys {

    private SaslInspectionLoggingKeys() {
    }

    /**
     * The Kafka API key identifying the request or response type.
     */
    public static final String API_KEY = "apiKey";

    /**
     * The disposition of the SASL authentication (success/failure).
     */
    public static final String AUTHENTICATION_DISPOSITION = "authenticationDisposition";

    /**
     * Current state of the SASL authentication process.
     */
    public static final String AUTHENTICATION_STATE = "authenticationState";

    /**
     * The authorised identity after SASL authentication.
     */
    public static final String AUTHORIZATION_ID = "authorizationId";

    /**
     * Kafka client identifier provided by the connecting client.
     */
    public static final String CLIENT_ID = "clientId";

    /**
     * SASL mechanism requested by the client.
     */
    public static final String CLIENT_MECHANISM = "clientMechanism";

    /**
     * SASL mechanisms supported by both client and server.
     */
    public static final String COMMON_MECHANISMS = "commonMechanisms";

    /**
     * Error message or exception description.
     */
    public static final String ERROR = "error";

    /**
     * Kafka protocol error code.
     */
    public static final String ERROR_CODE = "errorCode";

    /**
     * The SASL mechanism name (e.g., PLAIN, SCRAM-SHA-256).
     */
    public static final String MECHANISM = "mechanism";

    /**
     * Status of SASL mechanism negotiation.
     */
    public static final String MECHANISM_STATUS = "mechanismStatus";

    /**
     * Outcome of a SASL authentication attempt.
     */
    public static final String OUTCOME = "outcome";

    /**
     * SASL mechanism proposed for authentication.
     */
    public static final String PROPOSED_MECHANISM = "proposedMechanism";

    /**
     * SASL mechanisms supported by the server.
     */
    public static final String SERVER_MECHANISMS = "serverMechanisms";

    /**
     * The unique identifier for a client session with the proxy.
     */
    public static final String SESSION_ID = "sessionId";
}
