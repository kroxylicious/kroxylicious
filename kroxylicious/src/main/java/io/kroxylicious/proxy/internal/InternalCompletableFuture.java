/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Implementation of CompletableFuture that uses the given {@link Executor}
 * for async method invocations that do not specify one.
 * <br/>
 * @param <T> The result type returned by this future's {@code join}
 * and {@code get} methods.
 * @see InternalCompletionStage
 */
class InternalCompletableFuture<T> extends CompletableFuture<T> {

    private final Executor executor;

    InternalCompletableFuture(Executor executor) {
        this.executor = Objects.requireNonNull(executor);
    }

    /**
     * Returns a new incomplete InternalCompletableFuture of the type to be
     * returned by a CompletionStage method.
     *
     * @param <U> the type of the value
     * @return a new CompletableFuture
     */
    @Override
    public <U> CompletableFuture<U> newIncompleteFuture() {
        return new InternalCompletableFuture<>(executor);
    }

    /**
     * Returns the default Executor used for async methods that do not specify an Executor.
     *
     * @return the default executor
     */
    @Override
    public Executor defaultExecutor() {
        return executor;
    }

    /**
     * Returns a new CompletionStage that is completed normally with
     * the same value as this CompletableFuture when it completes
     * normally, and cannot be independently completed or otherwise
     * used in ways not defined by the methods of interface {@link
     * CompletionStage}.  If this CompletableFuture completes
     * exceptionally, then the returned CompletionStage completes
     * exceptionally with a CompletionException with this exception as
     * cause.
     * <br/>
     * The return CompletionStage implementation disallows the use
     * of {@link #toCompletableFuture()} ()}
     */
    @Override
    public CompletionStage<T> minimalCompletionStage() {
        return new InternalCompletionStage<>(this);
    }

    /**
     * Returns a new CompletableFuture that is already completed with the given value.
     * @param value – the value
     * @param <U> the type of the value
     * @return the completed CompletableFuture
     */
    public static <U> CompletableFuture<U> completedFuture(Executor executor, U value) {
        var f = new InternalCompletableFuture<U>(executor);
        f.complete(value);
        return f;
    }
}
