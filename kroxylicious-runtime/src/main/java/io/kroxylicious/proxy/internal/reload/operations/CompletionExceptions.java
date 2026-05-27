/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload.operations;

import java.util.concurrent.CompletionException;

final class CompletionExceptions {
    private CompletionExceptions() {
    }

    /**
     * Returns the underlying cause if {@code t} is a {@link CompletionException} wrapper.
     * Otherwise returns {@code t} itself. {@code CompletableFuture.join()} wraps every
     * non-{@code RuntimeException} cause in {@code CompletionException}; unwrapping lets
     * call sites surface the true cause in logs and error records.
     */
    static Throwable unwrap(Throwable t) {
        return t instanceof CompletionException ce && ce.getCause() != null ? ce.getCause() : t;
    }
}
