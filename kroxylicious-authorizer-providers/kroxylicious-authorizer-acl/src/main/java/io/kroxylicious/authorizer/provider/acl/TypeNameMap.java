/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import edu.umd.cs.findbugs.annotations.Nullable;

public class TypeNameMap<T, V> {

    public enum Predicate {
        TYPE_EQUAL_NAME_ANY,
        TYPE_EQUAL_NAME_EQUAL,
        TYPE_EQUAL_NAME_STARTS_WITH
    }

    record ClassNameKey<T>(
                           Class<? extends T> type,
                           Predicate predicate,
                           @Nullable String name)
            implements Comparable<ClassNameKey<T>> {

        ClassNameKey {
            Objects.requireNonNull(type);
        }

        @Override
        public int compareTo(ClassNameKey<T> o) {
            var cmp = this.type.getName().compareTo(o.type.getName());
            if (cmp == 0) {
                cmp = this.predicate.compareTo(o.predicate);
            }
            if (cmp == 0) {
                if (this.name == null) {
                    cmp = o.name == null ? 0 : -1;
                }
                else if (o.name == null) {
                    cmp = 1;
                }
                else {
                    cmp = this.name.compareTo(o.name);
                }
            }
            return cmp;
        }

        @Override
        public String toString() {
            return "(type=" + type +
                    ", name='" + name + '\'' +
                    ", predicate=" + predicate +
                    ')';
        }
    }

    private final TreeMap<ClassNameKey<T>, V> map = new TreeMap<>();

    @Override
    public String toString() {
        return "TypeNameMap{" +
                "map=" + map +
                '}';
    }

    public V computeIfAbsent(Class<? extends T> type, String name, Predicate predicate, Supplier<V> value) {
        return map.computeIfAbsent(new ClassNameKey<>(type, predicate, predicate == TypeNameMap.Predicate.TYPE_EQUAL_NAME_ANY ? null : name),
                p1 -> value.get());
    }

    public V compute(Class<? extends T> type, @Nullable String name, Predicate predicate, UnaryOperator<V> value) {
        return map.compute(new ClassNameKey<>(type, predicate, predicate == TypeNameMap.Predicate.TYPE_EQUAL_NAME_ANY ? null : name),
                (k, v) -> value.apply(v));
    }

    public @Nullable V lookup(Class<? extends T> type, Predicate predicate, @Nullable String name) {
        return switch (predicate) {
            case TYPE_EQUAL_NAME_EQUAL, TYPE_EQUAL_NAME_ANY -> {
                V v = map.get(new ClassNameKey<>(type, predicate, name));
                yield v;
            }
            case TYPE_EQUAL_NAME_STARTS_WITH -> {
                var entry = map.floorEntry(new ClassNameKey<>(type, Predicate.TYPE_EQUAL_NAME_STARTS_WITH, name));
                if (entry != null
                        && entry.getKey().predicate() == predicate
                        && entry.getKey().name != null
                        && name.startsWith(entry.getKey().name)) { // check the entry we found is a prefix for this thing
                    yield entry.getValue();
                }
                yield null;
            }
        };
    }

}
