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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.tag.VisibleForTesting;

/**
 * Applies standard patterns to futures to ensure consistent behaviour across async execution.
 */
class PromiseFactory {

    private final ScheduledExecutorService executorService;
    private final long timeout;
    private final TimeUnit timeoutUnit;
    private final Logger logger;

    /**
     *
     * @param executorService Used to schedule background tasks such as timeouts.
     * @param timeout Factory wide limit on how long to wait for a given future to complete.
     * @param timeoutUnit Defines the unit for the timeout period
     * @param loggerName Which logger should the factory use when logging events.
     */
    PromiseFactory(ScheduledExecutorService executorService, long timeout, TimeUnit timeoutUnit, String loggerName) {
        this.executorService = executorService;
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
        this.logger = LoggerFactory.getLogger(loggerName);
    }

    <T> CompletableFuture<T> newPromise() {
        return new InternalCompletableFuture<>(executorService);
    }

    /**
     * Ensure that a new promise is completed within the factories configured time limit
     * @param exceptionMessageGenerator a callable to be invoked when the time limit expires.
     * @return a time limited future.
     * @param <T> the type of the result of the future.
     */
    <T> CompletableFuture<T> newTimeLimitedPromise(Callable<String> exceptionMessageGenerator) {
        return wrapWithTimeLimit(newPromise(), exceptionMessageGenerator);
    }

    /**
     * Ensure that the supplied promise is completed within the factories configured time limit
     * @param promise the promise to be completed within the specified time.
     * @param exceptionMessageGenerator a callable to be invoked when the time limit expires.
     * @return a time limited future.
     * @param <T> the type of the result of the future.
     */
    <T> CompletableFuture<T> wrapWithTimeLimit(CompletableFuture<T> promise, Callable<String> exceptionMessageGenerator) {
        var timeoutFuture = executorService.schedule(timeoutTask(promise, exceptionMessageGenerator), timeout, timeoutUnit);
        promise.whenComplete((p, throwable) -> timeoutFuture.cancel(false));
        return promise;
    }

    @VisibleForTesting
    protected <T> Runnable timeoutTask(CompletableFuture<T> promise, Callable<String> exceptionMessageGenerator) {
        return () -> {
            final String message;
            try {
                message = exceptionMessageGenerator.call();

                boolean newlyCompleted = promise.completeExceptionally(new TimeoutException(message));
                if (newlyCompleted) {
                    logger.warn(message);
                }
                else {
                    logger.trace("Promise was completed before timeout (message on timeout would have been \"{}\")", message);
                }
            }
            catch (Exception e) {
                boolean newlyCompleted = promise.completeExceptionally(new TimeoutException("Promise Timed out"));
                if (newlyCompleted) {
                    logger.warn("Timeout exceptionMessageGenerator failed with {}. The promise has still been timed out.", e.getMessage(), e);
                }
                else {
                    logger.trace("Timeout exceptionMessageGenerator failed with {}, but the promise was already complete.", e.getMessage(), e);
                }
            }
        };
    }
}
