/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A CompletionStage implementation with guard rails so that chained work is executed on the
 * event loop.
 */
class InternalCompletionStage<T> implements CompletionStage<T> {

    private final CompletionStage<T> completionStage;

    InternalCompletionStage(CompletionStage<T> completionStage) {
        this.completionStage = completionStage;
    }

    private <U> CompletionStage<U> wrap(CompletionStage<U> completionStage) {
        return completionStage instanceof InternalCompletionStage ? completionStage : new InternalCompletionStage<>(completionStage);
    }

    private <U> CompletionStage<U> unwrap(CompletionStage<U> completionStage) {
        return completionStage instanceof InternalCompletionStage ? completionStage.toCompletableFuture() : completionStage;
    }

    @Override
    public <U> CompletionStage<U> thenApply(Function<? super T, ? extends U> fn) {
        return wrap(completionStage.thenApply(fn));
    }

    @Override
    public <U> CompletionStage<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
        return wrap(completionStage.thenApplyAsync(fn));
    }

    @Override
    public <U> CompletionStage<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
        return wrap(completionStage.thenApplyAsync(fn, executor));
    }

    @Override
    public CompletionStage<Void> thenAccept(Consumer<? super T> action) {
        return wrap(completionStage.thenAccept(action));
    }

    @Override
    public CompletionStage<Void> thenAcceptAsync(Consumer<? super T> action) {
        return wrap(completionStage.thenAcceptAsync(action));
    }

    @Override
    public CompletionStage<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
        return wrap(completionStage.thenAcceptAsync(action, executor));
    }

    @Override
    public CompletionStage<Void> thenRun(Runnable action) {
        return wrap(completionStage.thenRun(action));
    }

    @Override
    public CompletionStage<Void> thenRunAsync(Runnable action) {
        return wrap(completionStage.thenRunAsync(action));
    }

    @Override
    public CompletionStage<Void> thenRunAsync(Runnable action, Executor executor) {
        return wrap(completionStage.thenRunAsync(action, executor));
    }

    @Override
    public <U, V> CompletionStage<V> thenCombine(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
        return wrap(completionStage.thenCombine(unwrap(other), fn));
    }

    @Override
    public <U, V> CompletionStage<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
        return wrap(completionStage.thenCombineAsync(unwrap(other), fn));
    }

    @Override
    public <U, V> CompletionStage<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn, Executor executor) {
        return wrap(completionStage.thenCombineAsync(unwrap(other), fn, executor));
    }

    @Override
    public <U> CompletionStage<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        return wrap(completionStage.thenAcceptBoth(unwrap(other), action));
    }

    @Override
    public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        return wrap(completionStage.thenAcceptBothAsync(unwrap(other), action));
    }

    @Override
    public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action, Executor executor) {
        return wrap(completionStage.thenAcceptBothAsync(unwrap(other), action, executor));
    }

    @Override
    public CompletionStage<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return wrap(completionStage.runAfterBoth(unwrap(other), action));
    }

    @Override
    public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return wrap(completionStage.runAfterBothAsync(unwrap(other), action));
    }

    @Override
    public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return wrap(completionStage.runAfterBothAsync(unwrap(other), action, executor));
    }

    @Override
    public <U> CompletionStage<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return wrap(completionStage.applyToEither(unwrap(other), fn));
    }

    @Override
    public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return wrap(completionStage.applyToEitherAsync(unwrap(other), fn));
    }

    @Override
    public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor) {
        return wrap(completionStage.applyToEitherAsync(unwrap(other), fn, executor));
    }

    @Override
    public CompletionStage<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return wrap(completionStage.acceptEither(unwrap(other), action));
    }

    @Override
    public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return wrap(completionStage.acceptEitherAsync(unwrap(other), action));
    }

    @Override
    public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor) {
        return wrap(completionStage.acceptEitherAsync(unwrap(other), action, executor));
    }

    @Override
    public CompletionStage<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return wrap(completionStage.runAfterEither(unwrap(other), action));
    }

    @Override
    public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return wrap(completionStage.runAfterEitherAsync(unwrap(other), action));
    }

    @Override
    public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return wrap(completionStage.runAfterEitherAsync(unwrap(other), action, executor));
    }

    @Override
    public <U> CompletionStage<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return wrap(completionStage.thenCompose(fn.andThen(this::unwrap)));
    }

    @Override
    public <U> CompletionStage<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
        return wrap(completionStage.thenComposeAsync(fn.andThen(this::unwrap)));
    }

    @Override
    public <U> CompletionStage<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor) {
        return wrap(completionStage.thenComposeAsync(fn.andThen(this::unwrap), executor));
    }

    @Override
    public <U> CompletionStage<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
        return wrap(completionStage.handle(fn));
    }

    @Override
    public <U> CompletionStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
        return wrap(completionStage.handleAsync(fn));
    }

    @Override
    public <U> CompletionStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
        return wrap(completionStage.handleAsync(fn, executor));
    }

    @Override
    public CompletionStage<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return wrap(completionStage.whenComplete(action));
    }

    @Override
    public CompletionStage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
        return wrap(completionStage.whenCompleteAsync(action));
    }

    @Override
    public CompletionStage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor) {
        return wrap(completionStage.whenCompleteAsync(action, executor));
    }

    @Override
    public CompletionStage<T> exceptionally(Function<Throwable, ? extends T> fn) {
        return wrap(completionStage.exceptionally(fn));
    }

    @Override
    public CompletionStage<T> exceptionallyAsync(Function<Throwable, ? extends T> fn) {
        return wrap(completionStage.exceptionallyAsync(fn));
    }

    @Override
    public CompletionStage<T> exceptionallyAsync(Function<Throwable, ? extends T> fn, Executor executor) {
        return wrap(completionStage.exceptionallyAsync(fn, executor));
    }

    @Override
    public CompletionStage<T> exceptionallyCompose(Function<Throwable, ? extends CompletionStage<T>> fn) {
        return wrap(completionStage.exceptionallyCompose(fn.andThen(this::unwrap)));
    }

    @Override
    public CompletionStage<T> exceptionallyComposeAsync(Function<Throwable, ? extends CompletionStage<T>> fn) {
        return wrap(completionStage.exceptionallyComposeAsync(fn.andThen(this::unwrap)));
    }

    @Override
    public CompletionStage<T> exceptionallyComposeAsync(Function<Throwable, ? extends CompletionStage<T>> fn, Executor executor) {
        return wrap(completionStage.exceptionallyComposeAsync(fn.andThen(this::unwrap), executor));
    }

    @Override
    public CompletableFuture<T> toCompletableFuture() {
        return completionStage.toCompletableFuture();
    }

}
