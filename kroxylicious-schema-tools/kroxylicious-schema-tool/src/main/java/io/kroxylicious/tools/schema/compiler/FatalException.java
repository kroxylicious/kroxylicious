/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.compiler;

/**
 * Exception used to terminate the compiler immediately.
 * Don't throw this directly; use {@link Diagnostics#reportFatal(String, Object...)}
 */
public class FatalException extends RuntimeException {
    public FatalException() {
    }
}
