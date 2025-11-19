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

    private final TreeMap<Lookupable2<T>, V> map = new TreeMap<>();

    @Override
    public String toString() {
        return "TypeNameMap{" +
                "map=" + map +
                '}';
    }

    public V compute(Lookupable2<T> key, UnaryOperator<V> value) {
        return map.compute(key,
                (k, v) -> value.apply(v));
    }

    public @Nullable V lookup(Lookupable2<T> lookupable) {

        if (lookupable instanceof AclAuthorizer.ResourceMatcherNameEquals || lookupable instanceof AclAuthorizer.ResourceMatcherAnyOfType<T>) {
            V v = map.get(lookupable);
            return v;
        }
        else if (lookupable instanceof AclAuthorizer.ResourceMatcherNameStarts<T>) {
            var entry = map.floorEntry(lookupable);
            if (entry != null
                    && entry.getKey() instanceof AclAuthorizer.ResourceMatcherNameStarts
                    && entry.getKey().operand() != null
                    && lookupable.operand().startsWith(entry.getKey().operand())) { // check the entry we found is a prefix for this thing
                return entry.getValue();
            }
            return null;
        }
        throw new IllegalArgumentException("Unknown lookupable: " + lookupable);

//        return switch (lookupable.predicate()) {
//            case TYPE_EQUAL_NAME_EQUAL, TYPE_EQUAL_NAME_ANY -> {
//                V v = map.get(lookupable);
//                yield v;
//            }
//            case TYPE_EQUAL_NAME_STARTS_WITH -> {
//                var entry = map.floorEntry(lookupable);
//                if (entry != null
//                        && entry.getKey().predicate() == lookupable.predicate()
//                        && entry.getKey().operand() != null
//                        && lookupable.operand().startsWith(entry.getKey().operand())) { // check the entry we found is a prefix for this thing
//                    yield entry.getValue();
//                }
//                yield null;
//            }
//        };
    }

}
