/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

public class InvalidResourceException extends RuntimeException {

    public InvalidResourceException(String message) {
        super(message);
    }

}
