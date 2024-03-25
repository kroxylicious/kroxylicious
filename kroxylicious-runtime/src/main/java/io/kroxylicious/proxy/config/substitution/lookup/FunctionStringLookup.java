/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config.substitution.lookup;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * A function-based lookup where the request for a lookup is answered by applying that function with a key.
 *
 * @param <V> A function's input type
 *
 */
public final class FunctionStringLookup<V> implements StringLookup {

    /**
     * Creates a new instance backed by a Function.
     *
     * @param <R> the function's input type
     * @param function the function, may be null.
     * @return a new instance backed by the given function.
     */
    static <R> FunctionStringLookup<R> on(final Function<String, R> function) {
        return new FunctionStringLookup<>(function);
    }

    /**
     * Creates a new instance backed by a Map. Used by the default lookup.
     *
     * @param <V> the map's value type.
     * @param map the map of keys to values, may be null.
     * @return a new instance backed by the given map.
     */
    public static <V> FunctionStringLookup<V> on(final Map<String, V> map) {
        return on(k -> map.get(k));
    }

    /**
     * Function.
     */
    private final Function<String, V> function;

    /**
     * Creates a new instance backed by a Function.
     *
     * @param function the function, may be null.
     */
    private FunctionStringLookup(final Function<String, V> function) {
        this.function = function;
    }

    /**
     * Looks up a String key by applying the function.
     * <p>
     * If the function is null, then null is returned. The function result object is converted to a string using
     * toString().
     * </p>
     *
     * @param key the key to be looked up, may be null.
     * @return The function result as a string, may be null.
     */
    @Override
    public String lookup(final String key) {
        if (function == null) {
            return null;
        }
        final V obj;
        try {
            obj = function.apply(key);
        }
        catch (final SecurityException | NullPointerException | IllegalArgumentException e) {
            // Squelched. All lookup(String) will return null.
            // Could be a ConcurrentHashMap and a null key request
            return null;
        }
        return Objects.toString(obj, null);
    }

    @Override
    public String toString() {
        return super.toString() + " [function=" + function + "]";
    }

}
