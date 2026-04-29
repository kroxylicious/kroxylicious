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

    public static final String NAMESPACE = "namespace";
    public static final String NAME = "name";
    public static final String KIND = "kind";
    public static final String ERROR = "error";
}
