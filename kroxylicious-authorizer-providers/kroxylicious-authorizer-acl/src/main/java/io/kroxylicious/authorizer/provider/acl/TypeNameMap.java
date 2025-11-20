/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.TreeMap;
import java.util.function.UnaryOperator;

import edu.umd.cs.findbugs.annotations.Nullable;

class TypeNameMap<T, V> {

    private final TreeMap<OrderedKey<T>, V> map = new TreeMap<>();

    @Override
    public String toString() {
        return "TypeNameMap{" +
                "map=" + map +
                '}';
    }

    public V addApply(OrderedKey<T> key, UnaryOperator<V> value) {
        return map.compute(key,
                (k, v) -> value.apply(v));
    }

    public @Nullable V matchingOperations(OrderedKey<T> lookupable) {

        if (lookupable instanceof ResourceMatcherNameEquals || lookupable instanceof ResourceMatcherAnyOfType<T>) {
            return map.get(lookupable);
        }
        else if (lookupable instanceof ResourceMatcherNameStarts<T>) {
            var entry = map.floorEntry(lookupable);
            if (entry != null
                    && entry.getKey() instanceof ResourceMatcherNameStarts
                    && entry.getKey().operand() != null
                    && lookupable.operand().startsWith(entry.getKey().operand())) { // check the entry we found is a prefix for this thing
                return entry.getValue();
            }
            return null;
        }
        throw new IllegalArgumentException("Unknown lookupable: " + lookupable);
    }

}
