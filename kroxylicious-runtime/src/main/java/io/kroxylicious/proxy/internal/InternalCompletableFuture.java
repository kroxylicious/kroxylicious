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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import io.netty.util.concurrent.ThreadAwareExecutor;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Implementation of CompletableFuture that executes all chained work on a specific
 * event loop.
 * <br/>
 * @param <T> The result type returned by this future's {@code join}
 * and {@code get} methods.
 * @see InternalCompletionStage
 */
class InternalCompletableFuture<T> extends CompletableFuture<T> {

    private final ThreadAwareExecutor executor;

    InternalCompletableFuture(ThreadAwareExecutor executor) {
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

    private boolean inExecutorThread() {
        return executor.isExecutorThread(Thread.currentThread());
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
    public static <U> CompletableFuture<U> completedFuture(ThreadAwareExecutor executor, @Nullable U value) {
        var f = new InternalCompletableFuture<U>(executor);
        f.complete(value);
        return f;
    }

    @Override
    public <U> CompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
        return super.thenCompose(t -> {
            if (inExecutorThread()) {
                return super.thenApply(fn);
            }
            else {
                return super.thenApplyAsync(fn);
            }
        });
    }

    @Override
    public CompletableFuture<Void> thenAccept(Consumer<? super T> action) {
        return super.thenCompose(t -> {
            if (inExecutorThread()) {
                return super.thenAccept(action);
            }
            else {
                return super.thenAcceptAsync(action);
            }
        });
    }

    @Override
    public CompletableFuture<Void> thenRun(Runnable action) {
        return super.thenCompose(t -> {
            if (inExecutorThread()) {
                return super.thenRun(action);
            }
            else {
                return super.thenRunAsync(action);
            }
        });
    }

    @Override
    public CompletableFuture<T> exceptionally(Function<Throwable, ? extends T> fn) {
        return super.exceptionallyCompose(t -> {
            if (inExecutorThread()) {
                return super.exceptionally(fn);
            }
            else {
                return super.exceptionallyAsync(fn);
            }
        });
    }

    @Override
    public CompletableFuture<T> exceptionallyCompose(Function<Throwable, ? extends CompletionStage<T>> fn) {
        return super.exceptionallyCompose(t -> {
            if (inExecutorThread()) {
                return super.exceptionallyCompose(fn);
            }
            else {
                return super.exceptionallyComposeAsync(fn);
            }
        });
    }

    @Override
    public <U, V> CompletableFuture<V> thenCombine(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
        // go async to ensure function executed on event loop
        return thenCombineAsync(other, fn);
    }

    @Override
    public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
        CompletionStage<? extends U> internalFuture = toInternalFuture(other);
        return super.thenCombineAsync(internalFuture, fn);
    }

    @Override
    public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn, Executor executor) {
        CompletionStage<? extends U> internalFuture = toInternalFuture(other);
        return super.thenCombineAsync(internalFuture, fn, executor);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        // go async to ensure function executed on event loop
        return thenAcceptBothAsync(other, action);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        CompletionStage<? extends U> internalFuture = toInternalFuture(other);
        return super.thenAcceptBothAsync(internalFuture, action);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action, Executor executor) {
        CompletionStage<? extends U> internalFuture = toInternalFuture(other);
        return super.thenAcceptBothAsync(internalFuture, action, executor);
    }

    @Override
    public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        // go async to ensure function executed on event loop
        return runAfterBothAsync(other, action);
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        CompletionStage<?> internalFuture = toInternalFuture(other);
        return super.runAfterBothAsync(internalFuture, action);
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        CompletionStage<?> internalFuture = toInternalFuture(other);
        return super.runAfterBothAsync(internalFuture, action, executor);
    }

    @Override
    public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        // go async to ensure function executed on event loop
        return applyToEitherAsync(other, fn);
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        CompletionStage<? extends T> internalFuture = toInternalFuture(other);
        return super.applyToEitherAsync(internalFuture, fn);
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor) {
        CompletionStage<? extends T> internalFuture = toInternalFuture(other);
        return super.applyToEitherAsync(internalFuture, fn, executor);
    }

    @Override
    public CompletableFuture<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
        // go async to ensure function executed on event loop
        return acceptEitherAsync(other, action);
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action) {
        CompletionStage<? extends T> internalFuture = toInternalFuture(other);
        return super.acceptEitherAsync(internalFuture, action);
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor) {
        CompletionStage<? extends T> internalFuture = toInternalFuture(other);
        return super.acceptEitherAsync(internalFuture, action, executor);
    }

    @Override
    public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        // go async to ensure function executed on event loop
        return runAfterEitherAsync(other, action);
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        CompletionStage<?> internalFuture = toInternalFuture(other);
        return super.runAfterEitherAsync(internalFuture, action);
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        CompletionStage<?> internalFuture = toInternalFuture(other);
        return super.runAfterEitherAsync(internalFuture, action, executor);
    }

    @Override
    public <U> CompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return super.thenCompose(t -> {
            if (inExecutorThread()) {
                return super.thenCompose(fn);
            }
            else {
                return super.thenComposeAsync(fn);
            }
        });
    }

    @Override
    public <U> CompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
        InternalCompletableFuture<T> internalFuture = new InternalCompletableFuture<>(executor);
        CompletableFuture<U> handleFuture = internalFuture.underlyingHandle(fn);
        whenCompleteDispatch(internalFuture);
        return handleFuture;
    }

    private <U> CompletableFuture<U> underlyingHandle(BiFunction<? super T, Throwable, ? extends U> fn) {
        return super.handle(fn);
    }

    @Override
    public CompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        InternalCompletableFuture<T> internalFuture = new InternalCompletableFuture<>(executor);
        CompletableFuture<T> whenCompleteFuture = internalFuture.underlyingWhenComplete(action);
        whenCompleteDispatch(internalFuture);
        return whenCompleteFuture;
    }

    private CompletableFuture<T> underlyingWhenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return super.whenComplete(action);
    }

    private void whenCompleteDispatch(CompletableFuture<T> incompleteFuture) {
        super.whenComplete((t, throwable) -> dispatch(t, throwable, incompleteFuture));
    }

    private <U> CompletionStage<? extends U> toInternalFuture(CompletionStage<? extends U> other) {
        CompletableFuture<U> incompleteFuture = newIncompleteFuture();
        other.whenComplete((u, throwable) -> dispatch(u, throwable, incompleteFuture));
        return incompleteFuture;
    }

    private <U> void dispatch(@Nullable U u, @Nullable Throwable throwable, CompletableFuture<U> incompleteFuture) {
        if (inExecutorThread()) {
            if (throwable != null) {
                incompleteFuture.completeExceptionally(throwable);
            }
            else {
                incompleteFuture.complete(u);
            }
        }
        else {
            executor.execute(() -> {
                if (throwable != null) {
                    incompleteFuture.completeExceptionally(throwable);
                }
                else {
                    incompleteFuture.complete(u);
                }
            });
        }
    }

}
