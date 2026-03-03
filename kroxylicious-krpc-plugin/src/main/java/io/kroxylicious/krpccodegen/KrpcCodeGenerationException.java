/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen;

/**
 * Thrown when there is some issue during KRPC code generation
 */
public class KrpcCodeGenerationException extends RuntimeException {
    /**
     * Create KrpcCodeGenerationException
     * @param message message
     */
    public KrpcCodeGenerationException(String message) {
        super(message);
    }
}
