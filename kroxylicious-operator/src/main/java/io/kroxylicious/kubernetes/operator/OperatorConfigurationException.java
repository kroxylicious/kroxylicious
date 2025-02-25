/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

/**
 * A problem with the operator itself (e.g. which prevents it being able to start up).
 * Such problems exist independently of any particular {@code Proxy} resource.
 */
public class OperatorConfigurationException extends RuntimeException {
    public OperatorConfigurationException(String msg, Exception cause) {
        super(msg, cause);
    }
}
