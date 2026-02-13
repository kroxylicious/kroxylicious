/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin.reload;

/**
 * Base exception for configuration reload operations.
 * Thrown when a reload operation fails for any reason.
 */
public class ReloadException extends RuntimeException {

    public ReloadException(String message) {
        super(message);
    }

    public ReloadException(String message, Throwable cause) {
        super(message, cause);
    }
}
