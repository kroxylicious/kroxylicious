/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

/**
 * Common keys for structured logging in kroxylicious-authorization.
 */
public class AuthorizationLoggingKeys {

    private AuthorizationLoggingKeys() {
    }

    /**
     * Actions that are allowed by the authorisation policy.
     */
    public static final String ALLOWED_ACTIONS = "allowedActions";

    /**
     * The Kafka API key identifying the request or response type.
     */
    public static final String API_KEY = "apiKey";

    /**
     * Fully-qualified authoriser class name.
     */
    public static final String AUTHORIZER_CLASS = "authorizerClass";

    /**
     * Maximum API version supported by the broker.
     */
    public static final String BROKER_MAX_VERSION = "brokerMaxVersion";

    /**
     * Minimum API version supported by the broker.
     */
    public static final String BROKER_MIN_VERSION = "brokerMinVersion";

    /**
     * Actions that are denied by the authorisation policy.
     */
    public static final String DENIED_ACTIONS = "deniedActions";

    /**
     * Kafka protocol error code.
     */
    public static final String ERROR_CODE = "errorCode";

    /**
     * Failures encountered during processing.
     */
    public static final String FAILURES = "failures";

    /**
     * Fully-qualified filter class name.
     */
    public static final String FILTER_CLASS = "filterClass";

    /**
     * Maximum API version supported.
     */
    public static final String MAX_SUPPORTED_VERSION = "maxSupportedVersion";

    /**
     * Minimum API version supported.
     */
    public static final String MIN_SUPPORTED_VERSION = "minSupportedVersion";

    /**
     * API version specified in the request.
     */
    public static final String REQUEST_API_VERSION = "requestApiVersion";

    /**
     * Minimum required API version for this operation.
     */
    public static final String REQUIRED_MIN_VERSION = "requiredMinVersion";

    /**
     * The unique identifier for a client session with the proxy.
     */
    public static final String SESSION_ID = "sessionId";

    /**
     * Authenticated subject containing principals.
     */
    public static final String SUBJECT = "subject";

    /**
     * Actions that are not supported by the authoriser.
     */
    public static final String UNSUPPORTED_ACTIONS = "unsupportedActions";
}
