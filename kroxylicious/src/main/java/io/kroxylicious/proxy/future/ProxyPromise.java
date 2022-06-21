/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.future;

import java.util.Objects;

import io.kroxylicious.proxy.internal.ProxyPromiseImpl;

/**
 * A future that can be completed
 *
 * @param <T> The type of the result value.
 */
public interface ProxyPromise<T> extends ProxyFuture<T> {
    /**
     * Returns a future that has completed successfully
     *
     * @param value The result of the future.
     * @param <T>   The type of the result.
     * @return A successful future with the given value.
     */
    public static <T> ProxyFuture<T> success(T value) {
        var promise = new ProxyPromiseImpl<T>();
        promise.complete(value);
        return promise;
    }

    /**
     * Returns a future that has failed
     *
     * @param cause The cause of the failure; never null
     * @param <T>   The type of the result.
     * @return The failed future with the given cause.
     */
    public static <T> ProxyPromise<T> error(Throwable cause) {
        Objects.requireNonNull(cause);
        var promise = new ProxyPromiseImpl<T>();
        promise.fail(cause);
        return promise;
    }

    /**
     * Complete this promise successfully
     *
     * @param value
     * @throws IllegalStateException If this promise has already been completed
     */
    public default void complete(T value) {
        if (!tryComplete(value)) {
            throw new IllegalStateException("Result is already complete");
        }
    }

    public boolean tryComplete(T value);

    public default void fail(Throwable t) {
        if (!tryFail(t)) {
            throw new IllegalStateException("Result is already complete");
        }
    }

    public boolean tryFail(Throwable t);

    /**
     * Convert this promise to a future.
     *
     * @return
     */
    public ProxyFuture<T> future();

}
