/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

/**
 * Thrown when there was a problem attempting to serialize or deserialize an annotation value
 */
public class AnnotationSerializationException extends RuntimeException {
    public AnnotationSerializationException(Exception cause) {
        super(cause);
    }
}
