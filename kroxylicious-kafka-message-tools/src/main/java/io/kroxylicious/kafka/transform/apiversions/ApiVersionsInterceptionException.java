/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform.apiversions;

/**
 * Thrown when there is some issue during ApiVersions interception
 */
public class ApiVersionsInterceptionException extends RuntimeException {
    public ApiVersionsInterceptionException(String message) {
        super(message);
    }
}
