/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Kroxylicious guarantees that each Filter instance has a single thread associated with it (the same
 * thread may be shared by many filter instances). The threading model is intended to make it safe to
 * update Filter state from the methods of the Filter called by the framework. However, if the Filter
 * hands work off to another thread and wishes to update filter state we lose that safety. So we want
 * to enable Filter's to access the Filter thread so that they can switch back to a context where it is
 * safe to update state, or schedule jobs that update filter state.
 */
public interface FilterThreadExecutor extends ScheduledExecutorService {

    /**
     * @return true if current thread is the Filter thread backing this executor
     */
    boolean isInFilterThread();

    /**
     * Create a CompletionStage that will be completed on the Filter Thread. Note that
     * work chained onto the resulting stage may be executed directly in the calling thread
     * if the stage is already completed at that point. Following the contract of CompletableFuture
     * <p>Actions supplied for dependent completions of non-async methods may be performed by the
     * thread that completes the current CompletableFuture, or by any other caller of a completion method.</p>
     *
     * @param inStage
     * @return a completion stage that will be completed by the Filter Thread
     * @param <T> result type
     */
    default <T> CompletionStage<T> completedOnFilterThread(CompletionStage<T> inStage) {
        CompletableFuture<T> future = new CompletableFuture<>();
        inStage.whenComplete((t, throwable) -> {
            if (isInFilterThread()) {
                if (throwable == null) {
                    future.complete(t);
                }
                else {
                    future.completeExceptionally(throwable);
                }
            }
            else {
                execute(() -> {
                    if (throwable == null) {
                        future.complete(t);
                    }
                    else {
                        future.completeExceptionally(throwable);
                    }
                });
            }
        });
        return future;
    }
}
