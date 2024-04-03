/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PromiseFactory {

    private final ScheduledExecutorService executorService;
    private final long timeout;
    private final TimeUnit timeoutUnit;

    public PromiseFactory(ScheduledExecutorService executorService, long timeout, TimeUnit timeoutUnit) {
        this.executorService = executorService;
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
    }

    <T> CompletableFuture<T> newPromise() {
        return new InternalCompletableFuture<>(executorService);
    }

    <T> CompletableFuture<T> newTimeLimitedPromise(Callable<String> messageGenerator) {
        final InternalCompletableFuture<T> promise = new InternalCompletableFuture<>(executorService);
        var timeoutFuture = executorService.schedule(() -> {
            final String message;
            try {
                message = messageGenerator.call();
                promise.completeExceptionally(new TimeoutException(message));
            }
            catch (Exception e) {
                promise.completeExceptionally(new TimeoutException("Promise Timed out"));

            }
        }, timeout, timeoutUnit);
        promise.whenComplete((p, throwable) -> timeoutFuture.cancel(false));
        return promise;
    }
}
