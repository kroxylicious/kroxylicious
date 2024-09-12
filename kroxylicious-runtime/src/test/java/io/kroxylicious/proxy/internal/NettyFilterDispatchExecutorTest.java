/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.ScheduledFuture;

import io.kroxylicious.proxy.filter.FilterDispatchExecutor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatObject;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.BOOLEAN;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * These tests check that work is delegated to a netty event loop. For pragmatic reasons,
 * to avoid re-testing that the numerous methods of the underlying executor actually execute their
 * Runnable/Callable and honour cancellation, many of the tests use a mock event loop. We
 * check that the parameters are delegated as expected to the mock, and that the result
 * from the mock is forwarded, untouched, to the client.
 */
class NettyFilterDispatchExecutorTest {

    @Test
    void nullEventLoopNotAllowed() {
        assertThatThrownBy(() -> NettyFilterDispatchExecutor.eventLoopExecutor(null))
                                                                                     .isInstanceOf(NullPointerException.class)
                                                                                     .hasMessage("eventLoop cannot be null");
    }

    @Test
    void getEventloop() {
        EventLoop eventLoop = new DefaultEventLoop();
        try {
            FilterDispatchExecutor dispatchExecutor = NettyFilterDispatchExecutor.eventLoopExecutor(eventLoop);
            assertThat(dispatchExecutor).isInstanceOfSatisfying(NettyFilterDispatchExecutor.class, ex -> {
                assertThat(ex.getEventLoop()).isSameAs(eventLoop);
            });
        }
        finally {
            eventLoop.shutdownGracefully();
        }
    }

    @Test
    void notInFilterDispatchThreadWhenInTestThread() {
        EventLoop eventLoop = new DefaultEventLoop();
        try {
            FilterDispatchExecutor dispatchExecutor = NettyFilterDispatchExecutor.eventLoopExecutor(eventLoop);
            assertThat(dispatchExecutor.isInFilterDispatchThread()).isFalse();
        }
        finally {
            eventLoop.shutdownGracefully();
        }
    }

    @Test
    void nullStageNotAllowed() {
        EventLoop eventLoop = new DefaultEventLoop();
        try {
            FilterDispatchExecutor dispatchExecutor = NettyFilterDispatchExecutor.eventLoopExecutor(eventLoop);
            assertThatThrownBy(() -> dispatchExecutor.completeOnFilterDispatchThread(null))
                                                                                           .isInstanceOf(NullPointerException.class)
                                                                                           .hasMessage("completionStage was null");
        }
        finally {
            eventLoop.shutdownGracefully();
        }
    }

    @Test
    void notInFilterDispatchThreadWhenInAnotherEventLoop() {
        EventLoop eventLoop = new DefaultEventLoop();
        EventLoop anotherEventLoop = new DefaultEventLoop();
        try {
            FilterDispatchExecutor dispatchExecutor = NettyFilterDispatchExecutor.eventLoopExecutor(eventLoop);
            Future<Boolean> inDispatchThreadFuture = anotherEventLoop.submit(dispatchExecutor::isInFilterDispatchThread);
            assertThat(inDispatchThreadFuture).succeedsWithin(5, TimeUnit.SECONDS, BOOLEAN).isEqualTo(false);
        }
        finally {
            eventLoop.shutdownGracefully();
            anotherEventLoop.shutdownGracefully();
        }
    }

    @Test
    void inFilterDispatchThreadWhenInEventLoop() {
        EventLoop eventLoop = new DefaultEventLoop();
        try {
            FilterDispatchExecutor dispatchExecutor = NettyFilterDispatchExecutor.eventLoopExecutor(eventLoop);
            Future<Boolean> inDispatchThreadFuture = eventLoop.submit(dispatchExecutor::isInFilterDispatchThread);
            assertThat(inDispatchThreadFuture).succeedsWithin(5, TimeUnit.SECONDS, BOOLEAN).isEqualTo(true);
        }
        finally {
            eventLoop.shutdownGracefully();
        }
    }

    @Test
    void inFilterDispatchThreadByDelegation() {
        EventLoop eventLoop = new DefaultEventLoop();
        try {
            FilterDispatchExecutor dispatchExecutor = NettyFilterDispatchExecutor.eventLoopExecutor(eventLoop);
            Future<Boolean> inDispatchThreadFuture = dispatchExecutor.submit(dispatchExecutor::isInFilterDispatchThread);
            assertThat(inDispatchThreadFuture).succeedsWithin(5, TimeUnit.SECONDS, BOOLEAN).isEqualTo(true);
        }
        finally {
            eventLoop.shutdownGracefully();
        }
    }

    @Test
    void completeOnFilterDispatchThreadWhenInEventLoop() {
        EventLoop eventLoop = new DefaultEventLoop();
        try {
            FilterDispatchExecutor dispatchExecutor = NettyFilterDispatchExecutor.eventLoopExecutor(eventLoop);
            CompletableFuture<Object> completionStage = new CompletableFuture<>();
            CompletionStage<Boolean> inDispatchThread = dispatchExecutor.completeOnFilterDispatchThread(completionStage)
                                                                        .thenApply(o -> dispatchExecutor.isInFilterDispatchThread());
            completionStage.complete("arbitrary");
            assertThat(chainStandardCompletableFuture(inDispatchThread)).succeedsWithin(5, TimeUnit.SECONDS, BOOLEAN).isEqualTo(true);
        }
        finally {
            eventLoop.shutdownGracefully();
        }
    }

    static <T> CompletableFuture<T> chainStandardCompletableFuture(CompletionStage<T> stage) {
        CompletableFuture<T> future = new CompletableFuture<>();
        stage.whenComplete((o, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(o);
            }
        });
        return future;
    }

    @Test
    void completeOnFilterDispatchThreadResultCannotBeConvertedToCompletableFuture() {
        EventLoop eventLoop = new DefaultEventLoop();
        try {
            FilterDispatchExecutor dispatchExecutor = NettyFilterDispatchExecutor.eventLoopExecutor(eventLoop);
            CompletableFuture<Object> completionStage = new CompletableFuture<>();
            CompletionStage<Object> inDispatchThread = dispatchExecutor.completeOnFilterDispatchThread(completionStage);
            assertThatThrownBy(inDispatchThread::toCompletableFuture)
                                                                     .isInstanceOf(UnsupportedOperationException.class)
                                                                     .hasMessage(
                                                                             "CompletableFuture usage disallowed, we don't want to block the event loop or allow unexpected completion"
                                                                     );
        }
        finally {
            eventLoop.shutdownGracefully();
        }
    }

    @Test
    void completeOnFilterDispatchThreadResultNotCompletableByClientByCasting() {
        EventLoop eventLoop = new DefaultEventLoop();
        try {
            FilterDispatchExecutor dispatchExecutor = NettyFilterDispatchExecutor.eventLoopExecutor(eventLoop);
            CompletableFuture<Object> completionStage = new CompletableFuture<>();
            CompletionStage<Object> inDispatchThread = dispatchExecutor.completeOnFilterDispatchThread(completionStage);
            assertThatObject(inDispatchThread).isNotInstanceOf(CompletableFuture.class);
        }
        finally {
            eventLoop.shutdownGracefully();
        }
    }

    @Test
    void completeOnFilterDispatchThreadResultChainedAsyncOperationsDefaultToDispatchThread() {
        EventLoop eventLoop = new DefaultEventLoop();
        try {
            FilterDispatchExecutor dispatchExecutor = NettyFilterDispatchExecutor.eventLoopExecutor(eventLoop);
            CompletableFuture<Object> completionStage = new CompletableFuture<>();
            CompletionStage<Object> inDispatchThread = dispatchExecutor.completeOnFilterDispatchThread(completionStage);
            CompletionStage<Boolean> chainedAsyncStage = inDispatchThread.thenApplyAsync(o -> dispatchExecutor.isInFilterDispatchThread());
            completionStage.complete("abc");
            assertThat(chainStandardCompletableFuture(chainedAsyncStage)).succeedsWithin(5, TimeUnit.SECONDS, BOOLEAN).isEqualTo(true);
        }
        finally {
            eventLoop.shutdownGracefully();
        }
    }

    // sanity check, future is completed as expected when the input future is completed by the dispatcher thread
    @Test
    void completeOnFilterDispatchThreadWhenInDispatchThread() {
        EventLoop eventLoop = new DefaultEventLoop();
        try {
            FilterDispatchExecutor dispatchExecutor = NettyFilterDispatchExecutor.eventLoopExecutor(eventLoop);
            CompletableFuture<Object> completionStage = new CompletableFuture<>();
            CompletionStage<Boolean> inDispatchThread = dispatchExecutor.completeOnFilterDispatchThread(completionStage)
                                                                        .thenApply(o -> dispatchExecutor.isInFilterDispatchThread());
            dispatchExecutor.execute(() -> completionStage.complete("arbitrary"));
            assertThat(chainStandardCompletableFuture(inDispatchThread)).succeedsWithin(5, TimeUnit.SECONDS, BOOLEAN).isEqualTo(true);
        }
        finally {
            eventLoop.shutdownGracefully();
        }
    }

    @Test
    void completeOnFilterDispatchThreadWhenCompletedByAnotherEventLoop() {
        EventLoop eventLoop = new DefaultEventLoop();
        EventLoop anotherLoop = new DefaultEventLoop();
        try {
            FilterDispatchExecutor dispatchExecutor = NettyFilterDispatchExecutor.eventLoopExecutor(eventLoop);
            CompletableFuture<Object> completionStage = new CompletableFuture<>();
            CompletionStage<Boolean> inDispatchThread = dispatchExecutor.completeOnFilterDispatchThread(completionStage)
                                                                        .thenApply(o -> dispatchExecutor.isInFilterDispatchThread());
            anotherLoop.execute(() -> completionStage.complete("arbitrary"));
            assertThat(chainStandardCompletableFuture(inDispatchThread)).succeedsWithin(5, TimeUnit.SECONDS, BOOLEAN).isEqualTo(true);
            // double-check we are on the expected eventloop
            CompletableFuture<Object> completionStage2 = new CompletableFuture<>();
            CompletionStage<Boolean> inExpectedEventLoop = dispatchExecutor.completeOnFilterDispatchThread(completionStage2)
                                                                           .thenApply(o -> eventLoop.inEventLoop() && !anotherLoop.inEventLoop());
            anotherLoop.execute(() -> completionStage2.complete("arbitrary"));
            assertThat(chainStandardCompletableFuture(inExpectedEventLoop)).succeedsWithin(5, TimeUnit.SECONDS, BOOLEAN).isEqualTo(true);
        }
        finally {
            eventLoop.shutdownGracefully();
            anotherLoop.shutdownGracefully();
        }
    }

    @Test
    void scheduleCallableDelegated() {
        EventLoop mock = Mockito.mock();
        FilterDispatchExecutor dispatch = NettyFilterDispatchExecutor.eventLoopExecutor(mock);
        Callable<String> callable = () -> "hi";
        ScheduledFuture<String> future = Mockito.mock();
        when(mock.schedule(callable, 1, TimeUnit.SECONDS)).thenReturn(future);
        Future<String> scheduled = dispatch.schedule(callable, 1, TimeUnit.SECONDS);
        assertThat(scheduled).isSameAs(future);
    }

    @Test
    void submitCallableDelegated() {
        EventLoop mock = Mockito.mock();
        FilterDispatchExecutor dispatch = NettyFilterDispatchExecutor.eventLoopExecutor(mock);
        Callable<String> callable = () -> "hi";
        ScheduledFuture<String> future = Mockito.mock();
        when(mock.submit(callable)).thenReturn(future);
        Future<String> scheduled = dispatch.submit(callable);
        assertThat(scheduled).isSameAs(future);
    }

    @Test
    void scheduleRunnableDelegated() {
        EventLoop mock = Mockito.mock();
        FilterDispatchExecutor dispatch = NettyFilterDispatchExecutor.eventLoopExecutor(mock);
        Runnable runnable = () -> {
        };
        ScheduledFuture<Void> future = Mockito.mock();
        doReturn(future).when(mock).schedule(runnable, 1, TimeUnit.SECONDS);
        Future<?> scheduled = dispatch.schedule(runnable, 1, TimeUnit.SECONDS);
        assertThat(scheduled).isSameAs(future);
    }

    @Test
    void submitRunnableDelegated() {
        EventLoop mock = Mockito.mock();
        FilterDispatchExecutor dispatch = NettyFilterDispatchExecutor.eventLoopExecutor(mock);
        Runnable runnable = () -> {
        };
        ScheduledFuture<?> future = Mockito.mock();
        doReturn(future).when(mock).submit(runnable);
        Future<?> scheduled = dispatch.submit(runnable);
        assertThat(scheduled).isSameAs(future);
    }

    @Test
    void submitRunnableResultDelegated() {
        EventLoop mock = Mockito.mock();
        FilterDispatchExecutor dispatch = NettyFilterDispatchExecutor.eventLoopExecutor(mock);
        Runnable runnable = () -> {
        };
        ScheduledFuture<String> future = Mockito.mock();
        doReturn(future).when(mock).submit(runnable, "result");
        Future<String> scheduled = dispatch.submit(runnable, "result");
        assertThat(scheduled).isSameAs(future);
    }

    @Test
    void executeDelegated() {
        EventLoop mock = Mockito.mock();
        FilterDispatchExecutor dispatch = NettyFilterDispatchExecutor.eventLoopExecutor(mock);
        Runnable runnable = () -> {
        };
        dispatch.execute(runnable);
        verify(mock).execute(runnable);
    }

    @Test
    void scheduleAtFixedRateDelegated() {
        EventLoop mock = Mockito.mock();
        FilterDispatchExecutor dispatch = NettyFilterDispatchExecutor.eventLoopExecutor(mock);
        Runnable runnable = () -> {
        };
        ScheduledFuture<Void> future = Mockito.mock();
        doReturn(future).when(mock).scheduleAtFixedRate(runnable, 1L, 2L, TimeUnit.SECONDS);
        Future<?> scheduled = dispatch.scheduleAtFixedRate(runnable, 1L, 2L, TimeUnit.SECONDS);
        assertThat(scheduled).isSameAs(future);
    }

    @Test
    void scheduleWithFixedDelayDelegated() {
        EventLoop mock = Mockito.mock();
        FilterDispatchExecutor dispatch = NettyFilterDispatchExecutor.eventLoopExecutor(mock);
        Runnable runnable = () -> {
        };
        ScheduledFuture<Void> future = Mockito.mock();
        doReturn(future).when(mock).scheduleWithFixedDelay(runnable, 1L, 2L, TimeUnit.SECONDS);
        Future<?> scheduled = dispatch.scheduleWithFixedDelay(runnable, 1L, 2L, TimeUnit.SECONDS);
        assertThat(scheduled).isSameAs(future);
    }

    @Test
    void isShutdownDelegated() {
        EventLoop mock = Mockito.mock();
        FilterDispatchExecutor dispatch = NettyFilterDispatchExecutor.eventLoopExecutor(mock);
        when(mock.isShutdown()).thenReturn(true);
        assertThat(dispatch.isShutdown()).isTrue();
        when(mock.isShutdown()).thenReturn(false);
        assertThat(dispatch.isShutdown()).isFalse();
    }

    @Test
    void isTerminatedDelegated() {
        EventLoop mock = Mockito.mock();
        FilterDispatchExecutor dispatch = NettyFilterDispatchExecutor.eventLoopExecutor(mock);
        when(mock.isTerminated()).thenReturn(true);
        assertThat(dispatch.isTerminated()).isTrue();
        when(mock.isTerminated()).thenReturn(false);
        assertThat(dispatch.isTerminated()).isFalse();
    }

    @Test
    void awaitTerminationDelegated() throws InterruptedException {
        EventLoop mock = Mockito.mock();
        FilterDispatchExecutor dispatch = NettyFilterDispatchExecutor.eventLoopExecutor(mock);
        when(mock.awaitTermination(1L, TimeUnit.MINUTES)).thenReturn(true);
        assertThat(dispatch.awaitTermination(1L, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void shutdownIsNoOp() {
        EventLoop mock = Mockito.mock();
        FilterDispatchExecutor dispatch = NettyFilterDispatchExecutor.eventLoopExecutor(mock);
        dispatch.shutdownNow();
        verifyNoInteractions(mock);
    }

    @Test
    void shutdownNowIsNoOp() {
        EventLoop mock = Mockito.mock();
        FilterDispatchExecutor dispatch = NettyFilterDispatchExecutor.eventLoopExecutor(mock);
        dispatch.shutdownNow();
        verifyNoInteractions(mock);
    }

    @Test
    void invokeAll() throws InterruptedException {
        EventLoop mock = Mockito.mock();
        FilterDispatchExecutor dispatch = NettyFilterDispatchExecutor.eventLoopExecutor(mock);
        Callable<String> callable = () -> "hi";
        ScheduledFuture<String> future = Mockito.mock();
        List<Callable<String>> callables = List.of(callable);
        List<ScheduledFuture<String>> futures = List.of(future);
        doReturn(futures).when(mock).invokeAll(callables);
        List<Future<String>> results = dispatch.invokeAll(callables);
        assertThat(results).isSameAs(futures);
    }

    @Test
    void invokeAny() throws InterruptedException, ExecutionException {
        EventLoop mock = Mockito.mock();
        FilterDispatchExecutor dispatch = NettyFilterDispatchExecutor.eventLoopExecutor(mock);
        Callable<String> callable = () -> "hi";
        List<Callable<String>> callables = List.of(callable);
        String result = "result";
        doReturn(result).when(mock).invokeAny(callables);
        String results = dispatch.invokeAny(callables);
        assertThat(results).isEqualTo(result);
    }

    @Test
    void invokeAnyWithTimeout() throws InterruptedException, ExecutionException, TimeoutException {
        EventLoop mock = Mockito.mock();
        FilterDispatchExecutor dispatch = NettyFilterDispatchExecutor.eventLoopExecutor(mock);
        Callable<String> callable = () -> "hi";
        List<Callable<String>> callables = List.of(callable);
        String result = "result";
        doReturn(result).when(mock).invokeAny(callables, 1L, TimeUnit.SECONDS);
        String results = dispatch.invokeAny(callables, 1L, TimeUnit.SECONDS);
        assertThat(results).isEqualTo(result);
    }

    @Test
    void invokeAllWithTimeout() throws InterruptedException {
        EventLoop mock = Mockito.mock();
        FilterDispatchExecutor dispatch = NettyFilterDispatchExecutor.eventLoopExecutor(mock);
        Callable<String> callable = () -> "hi";
        ScheduledFuture<String> future = Mockito.mock();
        List<Callable<String>> callables = List.of(callable);
        List<ScheduledFuture<String>> futures = List.of(future);
        doReturn(futures).when(mock).invokeAll(callables, 1L, TimeUnit.SECONDS);
        List<Future<String>> results = dispatch.invokeAll(callables, 1L, TimeUnit.SECONDS);
        assertThat(results).isSameAs(futures);
    }
}
