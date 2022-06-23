/*
 * Copyright (c) 2011-2019 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */

package io.kroxylicious.proxy.internal.future;

import java.util.Objects;
import java.util.function.Function;

import io.kroxylicious.proxy.future.AsyncResult;
import io.kroxylicious.proxy.future.Future;

/**
 * Future base implementation.
 *
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
abstract class FutureBase<T> implements FutureInternal<T> {

    /**
     * Create a future that hasn't completed yet
     */
    FutureBase() {
    }

    protected final void emitSuccess(T value, Listener<T> listener) {
        listener.onSuccess(value);
    }

    protected final void emitFailure(Throwable cause, Listener<T> listener) {
        listener.onFailure(cause);
    }

    @Override
    public <U> Future<U> compose(Function<T, Future<U>> successMapper, Function<Throwable, Future<U>> failureMapper) {
        Objects.requireNonNull(successMapper, "No null success mapper accepted");
        Objects.requireNonNull(failureMapper, "No null failure mapper accepted");
        Composition<T, U> operation = new Composition<>(successMapper, failureMapper);
        addListener(operation);
        return operation;
    }

    @Override
    public <U> Future<U> transform(Function<AsyncResult<T>, Future<U>> mapper) {
        Objects.requireNonNull(mapper, "No null mapper accepted");
        Transformation<T, U> operation = new Transformation<>(this, mapper);
        addListener(operation);
        return operation;
    }

    @Override
    public <U> Future<T> eventually(Function<Void, Future<U>> mapper) {
        Objects.requireNonNull(mapper, "No null mapper accepted");
        Eventually<T, U> operation = new Eventually<>(mapper);
        addListener(operation);
        return operation;
    }

    @Override
    public <U> Future<U> map(Function<T, U> mapper) {
        Objects.requireNonNull(mapper, "No null mapper accepted");
        Mapping<T, U> operation = new Mapping<>(mapper);
        addListener(operation);
        return operation;
    }

    @Override
    public <V> Future<V> map(V value) {
        FixedMapping<T, V> transformation = new FixedMapping<>(value);
        addListener(transformation);
        return transformation;
    }

    @Override
    public Future<T> otherwise(Function<Throwable, T> mapper) {
        Objects.requireNonNull(mapper, "No null mapper accepted");
        Otherwise<T> transformation = new Otherwise<>(mapper);
        addListener(transformation);
        return transformation;
    }

    @Override
    public Future<T> otherwise(T value) {
        FixedOtherwise<T> operation = new FixedOtherwise<>(value);
        addListener(operation);
        return operation;
    }
}
