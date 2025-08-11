/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.concurrent.atomic.AtomicReference;

import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

public class TestAttribute<U> implements Attribute<U> {
    final AtomicReference<U> map;
    private final AttributeKey<U> key;

    public TestAttribute(AttributeKey<U> key) {
        this.key = key;
        map = new AtomicReference<>();
    }

    @Override
    public AttributeKey<U> key() {
        return key;
    }

    @Override
    public U get() {
        return map.get();
    }

    @Override
    public void set(U value) {
        map.set(value);
    }

    @Override
    public U getAndSet(U value) {

        return map.getAndSet(value);
    }

    @Override
    public U setIfAbsent(U value) {
        return map.compareAndExchange(null, value);
    }

    @Override
    public U getAndRemove() {
        return map.compareAndExchange(map.get(), null);
    }

    @Override
    public boolean compareAndSet(U oldValue,
                                 U newValue) {
        return map.compareAndSet(oldValue, newValue);
    }

    @Override
    public void remove() {
        map.set(null);
    }
}
