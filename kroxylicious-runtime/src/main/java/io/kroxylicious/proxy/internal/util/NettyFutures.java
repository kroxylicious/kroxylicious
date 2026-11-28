/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.util;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.slf4j.Logger;

import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.EventExecutor;

/**
 * Utility methods for working with Netty executors and futures, including dispatching completion
 * work onto an event loop and ensuring that failures in terminal error paths are logged rather
 * than silently discarded.
 */
public final class NettyFutures {

    private NettyFutures() {
    }

    /**
     * Runs {@code task} on {@code executor}, inline if the calling thread is already on it and
     * queued otherwise. If the executor rejects the task, {@code onRejected} is invoked on the
     * calling thread instead of the task being run.
     *
     * @param executor the executor to run the task on
     * @param task the task to run
     * @param onRejected invoked with the rejection if the executor refuses the task
     */
    public static void executeOnEventLoop(EventExecutor executor, Runnable task, Consumer<RejectedExecutionException> onRejected) {
        Objects.requireNonNull(executor, "executor must not be null");
        Objects.requireNonNull(task, "task must not be null");
        Objects.requireNonNull(onRejected, "onRejected must not be null");
        if (executor.inEventLoop()) {
            task.run();
        }
        else {
            try {
                executor.execute(task);
            }
            catch (RejectedExecutionException e) {
                onRejected.accept(e);
            }
        }
    }

    /**
     * Invokes {@code completion} with the outcome of {@code stage} on {@code executor}, inline
     * if the calling thread is already on it and queued otherwise. If the executor rejects the
     * completion, {@code onRejected} is invoked on the thread that completed {@code stage} and
     * {@code completion} is not run.
     *
     * @param executor the executor to run the completion on
     * @param stage the stage to observe
     * @param completion the completion to run with the stage's outcome
     * @param onRejected invoked with the rejection if the executor refuses the completion
     * @param <T> the stage value type
     */
    public static <T> void whenCompleteOnEventLoop(EventExecutor executor,
                                                   CompletionStage<T> stage,
                                                   BiConsumer<? super T, ? super Throwable> completion,
                                                   Consumer<RejectedExecutionException> onRejected) {
        Objects.requireNonNull(stage, "stage must not be null");
        Objects.requireNonNull(completion, "completion must not be null");
        stage.whenComplete((result, error) -> executeOnEventLoop(executor, () -> completion.accept(result, error), onRejected));
    }

    /**
     * Returns a {@link ChannelFutureListener} that logs a warning when the future completes
     * unsuccessfully. Intended for terminal error paths where there is no recovery to perform
     * and no caller to propagate the result into, so that exceptions are not silently dropped.
     *
     * @param log the logger to use
     * @param operation a short description of the operation, recorded as a structured log key-value
     * @return a listener that logs on failure
     */
    public static ChannelFutureListener logFailure(Logger log, String operation) {
        return future -> {
            if (!future.isSuccess() && future.cause() != null) {
                log.atWarn()
                        .setCause(future.cause())
                        .addKeyValue("operation", operation)
                        .log("Netty channel operation failed");
            }
        };
    }
}
