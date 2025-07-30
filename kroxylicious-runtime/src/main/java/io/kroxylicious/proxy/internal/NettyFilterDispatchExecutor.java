/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.netty.channel.EventLoop;

import io.kroxylicious.proxy.filter.FilterDispatchExecutor;

import edu.umd.cs.findbugs.annotations.Nullable;

public class NettyFilterDispatchExecutor implements FilterDispatchExecutor {

    private final EventLoop eventLoop;

    private NettyFilterDispatchExecutor(EventLoop eventLoop) {
        Objects.requireNonNull(eventLoop, "eventLoop cannot be null");
        this.eventLoop = eventLoop;
    }

    public static FilterDispatchExecutor eventLoopExecutor(EventLoop loop) {
        return new NettyFilterDispatchExecutor(loop);
    }

    @Override
    public boolean isInFilterDispatchThread() {
        return eventLoop.inEventLoop();
    }

    EventLoop getEventLoop() {
        return eventLoop;
    }

    @Override
    public <T> CompletionStage<T> completeOnFilterDispatchThread(CompletionStage<T> completionStage) {
        Objects.requireNonNull(completionStage, "completionStage was null");
        CompletableFuture<T> future = new InternalCompletableFuture<>(this);
        completionStage.whenComplete((value, throwable) -> {
            if (isInFilterDispatchThread()) {
                forward(value, throwable, future);
            }
            else {
                execute(() -> forward(value, throwable, future));
            }
        });
        return future.minimalCompletionStage();
    }

    private static <T> void forward(T value, @Nullable Throwable throwable, CompletableFuture<T> future) {
        if (throwable != null) {
            future.completeExceptionally(throwable);
        }
        else {
            future.complete(value);
        }
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return eventLoop.schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return eventLoop.schedule(callable, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return eventLoop.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return eventLoop.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    @Override
    public void shutdown() {
        // no-op, eventLoop lifecycle is not owned by this executor that is made available to Filters
    }

    @Override
    public List<Runnable> shutdownNow() {
        // no-op, eventLoop lifecycle is not owned by this executor that is made available to Filters
        return List.of();
    }

    @Override
    public boolean isShutdown() {
        return eventLoop.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return eventLoop.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return eventLoop.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return eventLoop.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return eventLoop.submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return eventLoop.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return eventLoop.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return eventLoop.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return eventLoop.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return eventLoop.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        eventLoop.execute(command);
    }
}
