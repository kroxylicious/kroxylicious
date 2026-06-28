/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform;

/**
 * Thrown when there is some issue during an ApiVersions transformation
 */
public class ApiVersionsTransformationException extends RuntimeException {
    public ApiVersionsTransformationException(String message) {
        super(message);
    }
}
