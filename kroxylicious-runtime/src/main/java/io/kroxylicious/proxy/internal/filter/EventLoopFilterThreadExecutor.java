/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.netty.channel.EventLoop;

import io.kroxylicious.proxy.filter.FilterThreadExecutor;

public class EventLoopFilterThreadExecutor implements FilterThreadExecutor {

    private final EventLoop eventLoop;

    public EventLoopFilterThreadExecutor(EventLoop eventLoop) {
        this.eventLoop = eventLoop;
    }

    @Override
    public boolean isInFilterThread() {
        return eventLoop.inEventLoop();
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
        eventLoop.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return eventLoop.shutdownNow();
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
