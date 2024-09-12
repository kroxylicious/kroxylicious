/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.common;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Kroxylicious guarantees that there is a single thread associated with a Filter instance that is
 * responsible for calling that Filter's methods. It may be useful for a Filter to be able to switch
 * back to that thread to safely mutate filter member state.
 */
public class FilterThreadExecutor {
    private final Executor executor;

    public FilterThreadExecutor(@NonNull
    Executor executor) {
        Objects.requireNonNull(executor);
        this.executor = executor;
    }

    /**
     * A Filter may need to ensure some CompletionStages are completed by the Filter thread
     * so that they can safely mutate Filter state. This method will switch if the stage is not already
     * completed, the assumption being the developer is calling this method from the Filter thread.
     * If the stage is done then we return it as-is on the assumption further work chained from it will
     * be executed directly, otherwise we switch to the Filter thread.
     * @param stage stage
     * @return if the stage is done it is returned unmodified, otherwise returns a stage that will be
     * completed with the result of stage on the Filter thread
     * @param <T> result type
     */
    public <T> @NonNull CompletionStage<T> completingOnFilterThread(@NonNull
    CompletionStage<T> stage) {
        CompletableFuture<T> future = stage.toCompletableFuture();
        if (future.isDone()) {
            return stage;
        } else {
            // no-op to switch executor
            return future.whenCompleteAsync((t, throwable) -> {
            }, executor);
        }
    }

}
