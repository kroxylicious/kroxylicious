/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Utilities for comparing two {@code List<T>} values as if they were maps keyed by some identity
 * function {@code T -> K}. Used by the change-detection pipeline when a list in the configuration
 * model represents a semantically unordered collection of named entries (e.g. gateways inside a
 * virtual cluster, filter definitions at the top level). For such fields, the record-generated
 * {@code List.equals()} — which IS order-sensitive — would produce spurious "modified" results
 * when a user reorders YAML entries without changing semantics.
 * <p>
 * This utility does <b>not</b> apply to lists whose ordering carries semantic meaning (the
 * filter chain inside a cluster, {@code defaultFilters}). Those should continue to use
 * {@link Objects#equals} directly.
 */
public final class KeyedListEquality {

    private KeyedListEquality() {
    }

    /**
     * Order-insensitive equality: treats both lists as maps keyed by {@code key} and then
     * compares the two maps.
     * <p>
     * Null-handling mirrors {@link Objects#equals(Object, Object)}:
     * <ul>
     *   <li>both null → {@code true}</li>
     *   <li>one null, one non-null → {@code false}</li>
     *   <li>both non-null → compare by size, then element-by-element after indexing by {@code key}</li>
     * </ul>
     * Duplicate keys on either side throw {@link IllegalStateException} (the caller's model
     * should have enforced uniqueness already).
     */
    public static <T, K> boolean equal(@Nullable List<T> a, @Nullable List<T> b, Function<T, K> key) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (a.size() != b.size()) {
            return false;
        }
        return indexByKey(a, key).equals(indexByKey(b, key));
    }

    /**
     * Returns the set of keys whose associated value differs between the two lists.
     * Keys present on only one side count as "changed" (addition or removal).
     * <p>
     * Useful for the diff pattern: "which named entries need attention?" Callers typically
     * iterate the returned set and decide per key what to do (restart affected clusters, etc.).
     */
    public static <T, K> Set<K> changedKeys(@Nullable List<T> a, @Nullable List<T> b, Function<T, K> key) {
        Map<K, T> aMap = a == null ? Map.of() : indexByKey(a, key);
        Map<K, T> bMap = b == null ? Map.of() : indexByKey(b, key);

        // Union of keys from both sides. Objects.equals(null, non-null) is false, which
        // correctly flags add/remove as "changed" — we don't need separate paths for
        // "only in a", "only in b", and "different in both".
        Set<K> changed = new HashSet<>();
        Stream.concat(aMap.keySet().stream(), bMap.keySet().stream())
                .distinct()
                .forEach(k -> {
                    if (!Objects.equals(aMap.get(k), bMap.get(k))) {
                        changed.add(k);
                    }
                });
        return changed;
    }

    private static <T, K> Map<K, T> indexByKey(List<T> list, Function<T, K> key) {
        return list.stream().collect(Collectors.toMap(key, Function.identity()));
    }
}
