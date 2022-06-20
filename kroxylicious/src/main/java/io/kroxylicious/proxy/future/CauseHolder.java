/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.future;

/**
 * Wrapper for exception, used in {@link ProxyPromiseImpl}.
 * Package private so that clients can never complete a {@link ProxyFuture} with one.
 */
class CauseHolder {
    final Throwable cause;

    public CauseHolder(Throwable cause) {
        this.cause = cause;
    }
}
