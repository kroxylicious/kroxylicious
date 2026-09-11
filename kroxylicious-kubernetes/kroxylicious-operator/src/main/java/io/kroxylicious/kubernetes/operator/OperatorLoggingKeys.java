/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

/**
 * Common keys for use with structured logging.
 */
public class OperatorLoggingKeys {

    private OperatorLoggingKeys() {
    }

    /** Structured logging key for the Kubernetes resource namespace. */
    public static final String NAMESPACE = "namespace";
    /** Structured logging key for the Kubernetes resource name. */
    public static final String NAME = "name";
    /** Structured logging key for the Kubernetes resource kind. */
    public static final String KIND = "kind";
    /** Structured logging key for an error message. */
    public static final String ERROR = "error";
}
