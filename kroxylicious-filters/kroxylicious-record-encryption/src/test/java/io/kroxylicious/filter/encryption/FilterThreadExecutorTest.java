/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import io.kroxylicious.filter.encryption.common.FilterThreadExecutor;

import static org.assertj.core.api.Assertions.assertThat;

class FilterThreadExecutorTest {

    private final ExecutorService executorA = Executors.newSingleThreadExecutor(r -> new Thread(r, "a"));
    private final ExecutorService executorB = Executors.newSingleThreadExecutor(r -> new Thread(r, "b"));
    private final FilterThreadExecutor dispatchExecutor = new FilterThreadExecutor(executorB);

    @Test
    void switchExecutorIfDeferred_SuccessSwitchesThreadIfDeferred() {
        CompletableFuture<String> taskA = CompletableFuture.supplyAsync(() -> Thread.currentThread().getName(), executorA);
        CompletionStage<String> switched = dispatchExecutor.completingOnFilterThread(taskA);
        CompletionStage<Boolean> sameThread = switched.thenApply(thread -> thread.equals(Thread.currentThread().getName()));
        assertThat(sameThread).succeedsWithin(Duration.ofSeconds(5L)).isEqualTo(false);
    }

    @Test
    void switchExecutorIfDeferred_SuccessDoesNotSwitchThreadIfDone() {
        CompletableFuture<String> taskA = CompletableFuture.completedFuture(Thread.currentThread().getName());
        CompletionStage<String> switched = dispatchExecutor.completingOnFilterThread(taskA);
        CompletionStage<Boolean> sameThread = switched.thenApply(thread -> thread.equals(Thread.currentThread().getName()));
        assertThat(sameThread).succeedsWithin(Duration.ofSeconds(5L)).isEqualTo(true);
    }

    @Test
    void switchExecutorIfDeferred_ExceptionSwitchesThreadIfDeferred() {
        CompletableFuture<String> taskA = CompletableFuture.supplyAsync(() -> {
            throw new TestException(Thread.currentThread().getName());
        }, executorA);
        CompletionStage<String> switched = dispatchExecutor.completingOnFilterThread(taskA);
        CompletionStage<String> extractException = switched.exceptionally(throwable -> {
            Assertions.assertThat(throwable).cause().isInstanceOf(TestException.class);
            return ((TestException) throwable.getCause()).thread;
        });
        CompletionStage<Boolean> sameThread = extractException.thenApply(thread -> thread.equals(Thread.currentThread().getName()));
        assertThat(sameThread).succeedsWithin(Duration.ofSeconds(5L)).isEqualTo(false);
    }

    @Test
    void switchExecutorIfDeferred_ExceptionDoesNotSwitchThreadIfDone() {
        CompletableFuture<String> taskA = CompletableFuture.failedFuture(new TestException(Thread.currentThread().getName()));
        CompletionStage<String> switched = dispatchExecutor.completingOnFilterThread(taskA);
        CompletionStage<String> extractException = switched.exceptionally(throwable -> {
            assertThat(throwable).isInstanceOf(TestException.class);
            return ((TestException) throwable).thread;
        });
        CompletionStage<Boolean> sameThread = extractException.thenApply(thread -> thread.equals(Thread.currentThread().getName()));
        assertThat(sameThread).succeedsWithin(Duration.ofSeconds(5L)).isEqualTo(true);
    }

    private static class TestException extends RuntimeException {
        private final String thread;

        TestException(String thread) {
            super("test exception triggered from thread " + thread);
            this.thread = thread;
        }
    }
}
