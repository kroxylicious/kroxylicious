/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.ArrayList;
import java.util.Objects;
import java.util.function.Function;

import io.kroxylicious.proxy.future.FailedFutureException;
import io.kroxylicious.proxy.future.ProxyFuture;
import io.kroxylicious.proxy.future.ProxyPromise;
import io.kroxylicious.proxy.future.UncompletedFutureException;

/**
 * Implementation of the {@link ProxyPromise} interface.
 *
 * @param <T> The type of the result value.
 */
public class ProxyPromiseImpl<T> implements ProxyPromise<T>, ProxyFuture<T> {

    private static final Object NULL_VALUE = new Object();

    /**
     * The result of the future.
     * This is always null for unresolved futures.
     * When a future completes with a null result this will be {@link #NULL_VALUE}.
     * When a future completes exceptionally the throwable is wrapped in a {@link CauseHolder}
     * to disambiguate the case where the non-failure result is an exception.
     */
    private Object value;

    /**
     * Listeners to fire when the future completes (successfully or not).
     * null when there are no listeners
     * a {@link Listener<T>} instance when there is a single listener
     * a {@link ListenerArray<T>} when there is more than one listener
     */
    private Listener<T> listener;

    @Override
    public boolean isDone() {
        return value != null;
    }

    @Override
    public boolean isSuccess() {
        return value != null && !(value instanceof CauseHolder);
    }

    @Override
    public boolean isFailed() {
        return value != null && value instanceof CauseHolder;
    }

    @Override
    public T value() {
        if (value != null) {
            if (value == NULL_VALUE) {
                return null;
            }
            else if (value instanceof CauseHolder) {
                throw new FailedFutureException(((CauseHolder) value).cause);
            }
            else {
                return (T) value;
            }
        }
        throw new UncompletedFutureException();
    }

    @Override
    public Throwable cause() {
        if (value instanceof CauseHolder) {
            return ((CauseHolder) value).cause;
        }
        else if (value != null) {
            return null;
        }
        else {
            throw new UncompletedFutureException();
        }
    }

    void addListener(Listener<T> listener) {
        Object v;
        synchronized (this) {
            v = value;
            if (v == null) {
                if (this.listener == null) {
                    this.listener = listener;
                }
                else {
                    ListenerArray<T> listeners;
                    if (this.listener instanceof ListenerArray) {
                        listeners = (ListenerArray<T>) this.listener;
                    }
                    else {
                        listeners = new ListenerArray<>();
                        listeners.add(this.listener);
                        this.listener = listeners;
                    }
                    listeners.add(listener);
                }
                return;
            }
        }
        if (v instanceof CauseHolder) {
            emitFailure(((CauseHolder) v).cause, listener);
        }
        else {
            if (v == NULL_VALUE) {
                v = null;
            }
            emitSuccess((T) v, listener);
        }
    }

    @Override
    public <U> ProxyFuture<U> compose(Function<T, ProxyFuture<U>> successMapper, Function<Throwable, ProxyFuture<U>> failureMapper) {
        Objects.requireNonNull(successMapper, "No null success mapper accepted");
        Objects.requireNonNull(failureMapper, "No null failure mapper accepted");
        Composition<T, U> operation = new Composition<>(context, successMapper, failureMapper);
        addListener(operation);
        return operation;
    }


    @Override
    public <X> ProxyFuture<X> compose(Function<T, ProxyFuture<X>> mapper) {
        return compose(mapper, ProxyPromiseImpl::failedFuture);
    }

    public static <X> ProxyFuture<X> failedFuture(Throwable throwable) {
        var p = new ProxyPromiseImpl<X>();
        p.fail(throwable);
        return p.future();
    }

    public static <X> ProxyFuture<X> succeededFuture(X result) {
        var p = new ProxyPromiseImpl<X>();
        p.complete(result);
        return p.future();
    }

    private static final ProxyFuture<?> EMPTY;
    static {
        var p = new ProxyPromiseImpl<>();
        p.complete(null);
        EMPTY = p.future();
    }

    @SuppressWarnings("unchecked")
    public static <X> ProxyFuture<X> succeededFuture() {
        return (ProxyFuture<X>) EMPTY;
    }

    @Override
    public <X> ProxyPromise<X> map(Function<T, X> mapper) {
        var p = new ProxyPromiseImpl<X>();
        // TODO support adding listeners properly
        this.listener = new Listener<T>() {
            @Override
            public void onSuccess(T value) {
                p.tryComplete(mapper.apply(value));
            }

            @Override
            public void onFailure(Throwable failure) {
                p.tryFail(failure);
            }
        };
        return p;
    }

    @Override
    public boolean tryComplete(T result) {
        Listener<T> l;
        synchronized (this) {
            if (value != null) {
                return false;
            }
            value = result == null ? NULL_VALUE : result;
            l = listener;
            listener = null;
        }
        if (l != null) {
            emitSuccess(result, l);
        }
        return true;
    }

    @Override
    public boolean tryFail(Throwable cause) {
        Objects.requireNonNull(cause);
        Listener<T> l;
        synchronized (this) {
            if (value != null) {
                return false;
            }
            value = new CauseHolder(cause);
            l = listener;
            listener = null;
        }
        if (l != null) {
            emitFailure(cause, l);
        }
        return true;
    }

    private void emitFailure(Throwable cause, Listener<T> l) {
        Objects.requireNonNull(l).onFailure(cause);
    }

    protected final void emitSuccess(T value, Listener<T> listener) {
        // if (context != null && !context.isRunningOnContext()) {
        // context.execute(() -> {
        // ContextInternal prev = context.beginDispatch();
        // try {
        // listener.onSuccess(value);
        // } finally {
        // context.endDispatch(prev);
        // }
        // });
        // } else {
        Objects.requireNonNull(listener).onSuccess(value);
        // }
    }

    @Override
    public ProxyFuture<T> future() {
        return this;
    }

    /**
     * Wrapper for throwable, used in {@link ProxyPromiseImpl}.
     * Package private so that clients can never complete a {@link ProxyFuture} with one.
     */
    private static class CauseHolder {
        final Throwable cause;

        private CauseHolder(Throwable cause) {
            this.cause = cause;
        }
    }

    static interface Listener<T> {

        /**
         * Signal the success.
         *
         * @param value the value
         */
        void onSuccess(T value);

        /**
         * Signal the failure
         *
         * @param failure the failure
         */
        void onFailure(Throwable failure);
    }

    private static class ListenerArray<T> extends ArrayList<Listener<T>> implements Listener<T> {
        @Override
        public void onSuccess(T value) {
            for (Listener<T> handler : this) {
                handler.onSuccess(value);
            }
        }

        @Override
        public void onFailure(Throwable failure) {
            for (Listener<T> handler : this) {
                handler.onFailure(failure);
            }
        }
    }
}
