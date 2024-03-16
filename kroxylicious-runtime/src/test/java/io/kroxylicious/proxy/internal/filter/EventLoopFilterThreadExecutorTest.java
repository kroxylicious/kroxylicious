/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.netty.channel.DefaultEventLoop;

import static org.assertj.core.api.Assertions.assertThat;

class EventLoopFilterThreadExecutorTest {

    private final DefaultEventLoop eventLoop = new DefaultEventLoop();

    @AfterEach
    public void after() {
        eventLoop.shutdownGracefully();
    }

    @Test
    public void testWithCompletedFuture() {
        EventLoopFilterThreadExecutor executor = new EventLoopFilterThreadExecutor(eventLoop);
        CompletionStage<Long> future = executor.completedOnFilterThread(CompletableFuture.completedFuture(1L));
        assertThat(future).succeedsWithin(Duration.ZERO).isEqualTo(1L);
    }

    @Test
    public void testSuccessOnUncontrolledExecutorSwitchesThread() {
        EventLoopFilterThreadExecutor executor = new EventLoopFilterThreadExecutor(eventLoop);
        CompletableFuture<String> threadName = new CompletableFuture<>();
        CompletionStage<String> stage = executor.completedOnFilterThread(threadName);
        CompletionStage<Boolean> sameThreadStage = stage.thenApply(s -> {
            assertInEventLoop(eventLoop);
            return s.equals(Thread.currentThread().getName());
        });

        // complete threadname after the chain is established to avoid races where main thread can drive completion
        CompletableFuture.runAsync(() -> {
            assertNotInEventLoop(eventLoop);
            threadName.complete(Thread.currentThread().getName());
        });
        assertThat(sameThreadStage).succeedsWithin(Duration.ofSeconds(5L)).isEqualTo(false);
    }

    @Test
    public void testExceptionOnUncontrolledExecutorSwitchesThread() {
        EventLoopFilterThreadExecutor executor = new EventLoopFilterThreadExecutor(eventLoop);
        CompletableFuture<String> threadName = new CompletableFuture<>();
        CompletionStage<String> stage = executor.completedOnFilterThread(threadName);
        CompletionStage<String> extractNameFromException = stage.exceptionally(ex -> {
            assertInEventLoop(eventLoop);
            if (ex instanceof TestException testException) {
                return testException.threadName;
            }
            else {
                throw new RuntimeException("unexpected exception: " + ex);
            }
        });
        CompletionStage<Boolean> sameThreadStage = extractNameFromException.thenApply(s -> {
            assertInEventLoop(eventLoop);
            return s.equals(Thread.currentThread().getName());
        });

        // complete threadname after the chain is established to avoid races where main thread can drive completion
        CompletableFuture.runAsync(() -> {
            assertNotInEventLoop(eventLoop);
            threadName.completeExceptionally(new TestException(Thread.currentThread().getName()));
        });
        assertThat(sameThreadStage).succeedsWithin(Duration.ofSeconds(5L)).isEqualTo(false);
    }

    @Test
    public void testExceptionOnEventLoopExecutorRemainsOnThread() {
        EventLoopFilterThreadExecutor executor = new EventLoopFilterThreadExecutor(eventLoop);
        CompletableFuture<String> threadName = new CompletableFuture<>();
        CompletionStage<String> stage = executor.completedOnFilterThread(threadName);
        CompletionStage<String> extractNameFromException = stage.exceptionally(ex -> {
            assertInEventLoop(eventLoop);
            if (ex instanceof TestException testException) {
                return testException.threadName;
            }
            else {
                throw new RuntimeException("unexpected exception: " + ex);
            }
        });
        CompletionStage<Boolean> sameThreadStage = extractNameFromException.thenApply(s -> {
            assertInEventLoop(eventLoop);
            return s.equals(Thread.currentThread().getName());
        });

        // complete threadname after the chain is established to avoid races where main thread can drive completion
        CompletableFuture.runAsync(() -> {
            assertInEventLoop(eventLoop);
            threadName.completeExceptionally(new TestException(Thread.currentThread().getName()));
        }, eventLoop);
        assertThat(sameThreadStage).succeedsWithin(Duration.ofSeconds(5L)).isEqualTo(true);
    }

    @Test
    public void testSuccessOnEventLoopRemainsOnEventLoop() {
        EventLoopFilterThreadExecutor executor = new EventLoopFilterThreadExecutor(eventLoop);
        CompletableFuture<String> threadName = new CompletableFuture<>();
        CompletionStage<String> stage = executor.completedOnFilterThread(threadName);
        CompletionStage<Boolean> sameThreadStage = stage.thenApply(s -> {
            assertInEventLoop(eventLoop);
            return s.equals(Thread.currentThread().getName());
        });

        // complete threadname after the chain is established to avoid races where main thread can drive completion
        CompletableFuture.runAsync(() -> {
            assertInEventLoop(eventLoop);
            threadName.complete(Thread.currentThread().getName());
        }, eventLoop);
        assertThat(sameThreadStage).succeedsWithin(Duration.ofSeconds(5L)).isEqualTo(true);
    }

    private static void assertNotInEventLoop(DefaultEventLoop eventLoop) {
        assertThat(eventLoop.inEventLoop()).isFalse();
    }

    private static void assertInEventLoop(DefaultEventLoop eventLoop) {
        assertThat(eventLoop.inEventLoop()).isTrue();

    }

    private class TestException extends RuntimeException {
        private final String threadName;

        TestException(String threadName) {
            this.threadName = threadName;
        }
    }
}