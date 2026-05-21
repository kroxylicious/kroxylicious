/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.reload;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Outcome of a {@code KafkaProxy.reconfigure(Configuration)} call. Reports the per-component
 * failures encountered while reconfiguring; an empty {@link #errors()} collection indicates
 * the reconfigure succeeded with no failed components.
 *
 * <p>The interface is deliberately minimal: it only enumerates <em>what failed</em> and
 * <em>why</em>, and leaves any reaction to the caller. Failure-handling policy (whether to
 * shut down on partial failure, attempt a rollback, alert, or retry) is expressed by the
 * caller via the {@link java.util.concurrent.CompletableFuture#whenComplete} pattern on the
 * future returned by {@code reconfigure()}; the proxy itself takes no policy action based
 * on {@code errors()}.
 */
public interface ReconfigureResult {

    /**
     * Returns the per-component failures encountered while reconfiguring. One entry per
     * failed component (e.g. one virtual cluster, one referenced filter). Empty when the
     * reconfigure succeeded with no failed components.
     *
     * <p>The returned collection is immutable; iteration order is unspecified.
     */
    Collection<ReconfigureError> errors();

    /**
     * Convenience predicate equivalent to {@code !errors().isEmpty()}.
     */
    default boolean hasErrors() {
        return !errors().isEmpty();
    }

    /**
     * Returns a {@code ReconfigureResult} backed by an immutable copy of the given errors.
     * Intended for testing and for the proxy's internal implementation; embedders normally
     * receive {@code ReconfigureResult} instances via the future returned from
     * {@code KafkaProxy.reconfigure(Configuration)} and do not construct their own.
     *
     * @param errors per-component failures; must not be {@code null}, but may be empty
     * @return an immutable result whose {@link #errors()} reflects the supplied collection
     * @throws NullPointerException if {@code errors} is {@code null}
     */
    static ReconfigureResult of(Collection<ReconfigureError> errors) {
        Objects.requireNonNull(errors, "errors");
        List<ReconfigureError> snapshot = List.copyOf(errors);
        return () -> snapshot;
    }
}
