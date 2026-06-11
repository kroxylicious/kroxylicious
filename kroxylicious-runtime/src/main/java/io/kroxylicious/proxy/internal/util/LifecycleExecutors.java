/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.util;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helpers for running virtual-cluster lifecycle work on a dedicated executor.
 */
public final class LifecycleExecutors {

    /**
     * Thread name prefix for the lifecycle executor. Visible in stack traces and thread dumps.
     */
    public static final String LIFECYCLE_THREAD_NAME_PREFIX = "kroxylicious-vc-lifecycle";

    private LifecycleExecutors() {
    }

    /**
     * Creates the proxy's lifecycle executor. Single-threaded, daemon thread, named with
     * {@link #LIFECYCLE_THREAD_NAME_PREFIX}. Owned by the proxy and shut down on proxy close.
     */
    public static ExecutorService newLifecycleExecutor() {
        return Executors.newSingleThreadExecutor(new LifecycleThreadFactory());
    }

    /**
     * Submits a callable to the lifecycle executor and waits for its result. Used at every
     * VCM-construction site (startup and reconfigure) so that {@code FilterFactory.initialize()}
     * runs on the lifecycle thread regardless of what thread the caller was on.
     *
     * <p>Exceptions thrown by the task are unwrapped: a {@link RuntimeException} thrown inside
     * the task is rethrown with the same type, preserving the caller's catch-by-type semantics.
     * Checked exceptions are wrapped in {@link CompletionException}.
     *
     * @throws RuntimeException the same RuntimeException the task threw, or a
     *         {@link CompletionException} wrapping a checked exception
     */
    @SuppressWarnings("java:S1181") // catching Throwable is intentional — see comment inside
    public static <T> T runOnLifecycle(Executor executor, Callable<T> task) {
        // Accepting Executor (not ExecutorService) keeps the test surface small — a synchronous
        // Runnable::run lambda is a valid Executor and lets unit tests run lifecycle work on the
        // calling thread without spinning up real threads.
        var future = new CompletableFuture<T>();
        executor.execute(() -> {
            try {
                future.complete(task.call());
            }
            catch (Throwable t) {
                // Catch Throwable (not Exception) so Errors thrown by the task are forwarded
                // to the caller via the future rather than escaping into the single-threaded
                // executor.
                future.completeExceptionally(t);
            }
        });
        try {
            return future.join();
        }
        catch (CompletionException e) {
            var cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw new CompletionException(cause);
        }
    }

    private static final class LifecycleThreadFactory implements ThreadFactory {
        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r) {
            var t = new Thread(r, LIFECYCLE_THREAD_NAME_PREFIX + "-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }
}
