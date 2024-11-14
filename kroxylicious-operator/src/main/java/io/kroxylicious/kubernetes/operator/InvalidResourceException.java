/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

/**
 * A primary resource that this not valid.
 * Typically, we want invalid resources to be rejected by the Kube API Server
 * because they're not schema valid. This exception is for cases where
 * the operator necessarily has responsibility for checking.
 */
public class InvalidResourceException extends RuntimeException {

    public InvalidResourceException(String message) {
        super(message);
    }

}
