/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Executor backed by the <b>Filter Dispatch Thread</b>. That is the single thread associated with the Channel for
 * a Filter Instance. All invocations of that instance's methods are made by the Filter Dispatch Thread.
 * <p>
 * If all accesses/mutations of Filter Members happen on the Filter Dispatch Thread, then those interactions are
 * serial and threadsafe. This executor enables Authors to do work in uncontrolled threads and then return to
 * the Filter Dispatch Thread to take advantage of this convenient threading guarantee.
 * </p>
 * <p>
 * Note that implementations of FilterDispatchExecutor will not honour the shut-down methods {@link #shutdown()}
 * and {@link #shutdownNow()} and will throw a RuntimeException if those methods are called, as the client is
 * not allowed to shut down the Filter Dispatch Thread.
 * </p>
 * @see io.kroxylicious.proxy.filter
 */
public interface FilterDispatchExecutor extends ScheduledExecutorService {

    /**
     * @return Return true if {@link Thread#currentThread()} is this Filter Dispatch Thread, false otherwise
     */
    boolean isInFilterDispatchThread();

    /**
     * Switches to this Filter Dispatch Thread. The CompletionStage returned is an implementation
     * that discourages blocking work and will throw if the client attempts to call
     * {@link CompletionStage#toCompletableFuture()}. Any async work chained onto the
     * result CompletionStage using methods like {@link CompletionStage#thenApplyAsync(Function)}
     * will also be executed on the Filter Dispatch Thread.
     * @param stage input stage
     * @return a stage completed (both exceptionally and normally) by this Filter Dispatch Thread
     * @param <T> stage value type
     */
    <T> CompletionStage<T> completeOnFilterDispatchThread(@NonNull
    CompletionStage<T> stage);

}
