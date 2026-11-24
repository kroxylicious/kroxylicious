/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.function.UnaryOperator;

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

    /**
     * Returns the value of every key matching the given lookup key.
     * <ul>
     *     <li>A {@link ResourceMatcherNameEquals} or {@link ResourceMatcherAnyOfType} lookup matches
     *     the equal key, if any.</li>
     *     <li>A {@link ResourceMatcherNameStarts} lookup, whose operand is the name being looked up,
     *     matches every {@link ResourceMatcherNameStarts} key of the same type whose prefix the name
     *     starts with. Overlapping prefixes all match, and must all be applied by the caller: a
     *     shorter prefix can carry a rule (an operation, or a deny) that a longer one does not.</li>
     * </ul>
     * Keys of another type never match, so a rule for one principal or resource class never
     * applies to another class whose names happen to share a prefix.
     *
     * @param lookupable the lookup key
     * @return the matching values, empty if none match
     */
    public List<V> matchingValues(OrderedKey<T> lookupable) {
        if (lookupable instanceof ResourceMatcherNameEquals || lookupable instanceof ResourceMatcherAnyOfType<T>) {
            V value = map.get(lookupable);
            return value == null ? List.of() : List.of(value);
        }
        else if (lookupable instanceof ResourceMatcherNameStarts<T> starts) {
            List<V> result = new ArrayList<>();
            for (var entry : map.subMap(new ResourceMatcherNameStarts<>(starts.type(), ""), true, starts, true).entrySet()) {
                // Cast must be safe because OrderedKey.compareTo ensures submap contains only ResourceMatcherNameStarts
                var entryStarts = (ResourceMatcherNameStarts<?>) entry.getKey();
                if (starts.operand().startsWith(entryStarts.operand())) { // check the entry we found is a prefix for this thing
                    result.add(entry.getValue());
                }
            }
            Collections.reverse(result);
            return result;
        }
        throw new IllegalArgumentException("Unknown lookupable: " + lookupable);
    }

}
