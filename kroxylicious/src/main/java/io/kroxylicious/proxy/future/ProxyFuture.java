/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.future;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface ProxyFuture<T> {

    /**
     * Returns whether this future has completed, either normally, or abnormally.
     */
    boolean isDone();

    /**
     * Returns whether this future has completed normally.
     * This method returning true implies {@link #value()} will return the value.
     * @return true if this future has completed normally.
     */
    boolean isSuccess();

    /**
     * Returns whether this future has completed abnormally.
     * This method returning true implies {@link #cause()} will return the cause.
     * @return true if this future has completed abnormally.
     */
    boolean isFailed();

    /**
     * @return If this future {@linkplain #isSuccess() succeeded} this is the result, and may be null.
     * If this future {@linkplain #isFailed() failed}, this method will throw {@link FailedFutureException}.
     * If this future is not {@link #isDone() done} this is guaranteed to throw
     * an {@link UncompletedFutureException}
     */
    T value();

    /**
     * @return If this future {@linkplain #isFailed() failed}, this is the cause of an abnormal completion
     * and guaranteed to be non-null.
     * If this future {@linkplain #isSuccess() succeeded} this is guaranteed to be null.
     * If this future is not {@linkplain #isDone() done} this is guaranteed to throw
     * an {@link UncompletedFutureException}
     */
    Throwable cause();

    /**
     * Compose this future with a {@code mapper} function.<p>
     *
     * When this future (the one on which {@code compose} is called) succeeds, the {@code mapper} will be called with
     * the completed value and this mapper returns another future object. This returned future completion will complete
     * the future returned by this method call.<p>
     *
     * If the {@code mapper} throws an exception, the returned future will be failed with this exception.<p>
     *
     * When this future fails, the failure will be propagated to the returned future and the {@code mapper}
     * will not be called.
     *
     * @param mapper the mapper function
     * @return the composed future
     */
    <U> ProxyFuture<U> compose(Function<T, ProxyFuture<U>> mapper);

    <U> ProxyFuture<U> compose(BiFunction<Throwable, T, ProxyFuture<U>> mapper);

    /**
     * Add a handler to be notified of the result.
     * @param handler – the handler that will be called with the result
     * Returns:
     * @return a reference to this, so it can be used fluently
     */
    ProxyFuture<T> onComplete(BiConsumer<Throwable, T> handler);

    /**
     * Apply a {@code mapper} function on this future.<p>
     *
     * When this future succeeds, the {@code mapper} will be called with the completed value and this mapper
     * returns a value. This value will complete the future returned by this method call.<p>
     *
     * If the {@code mapper} throws an exception, the returned future will be failed with this exception.<p>
     *
     * When this future fails, the failure will be propagated to the returned future and the {@code mapper}
     * will not be called.
     *
     * @param mapper the mapper function
     * @return the mapped future
     */
    <U> ProxyFuture<U> map(Function<T, U> mapper);

    <U> ProxyFuture<U> map(BiFunction<Throwable, T, U> mapper);

    default <U> ProxyFuture<U> map(U value) {
        return map(x -> value);
    }

}
