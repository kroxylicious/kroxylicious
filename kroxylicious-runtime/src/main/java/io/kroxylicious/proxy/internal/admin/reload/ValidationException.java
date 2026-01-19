/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin.reload;

/**
 * Thrown when configuration validation fails during reload.
 * This includes YAML parsing errors, schema validation failures,
 * and semantic validation errors.
 */
public class ValidationException extends ReloadException {

    public ValidationException(String message) {
        super(message);
    }

    public ValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
