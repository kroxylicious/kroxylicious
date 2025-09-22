/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authorization.simple;

import java.util.EnumSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import io.kroxylicious.proxy.authorization.Operation;

public class TypePatternMatch {

    record Key(
            Class<?> c,
            Pattern p) implements Comparable<TypePatternMatch.Key> {

        @Override
        public int compareTo(Key o) {
            var cmp = c.getName().compareTo(o.c.getName());
            if (cmp == 0) {
                cmp = p.pattern().compareTo(o.p.pattern());
            }
            return cmp;
        }
    }

    TreeMap<Key, EnumSet<?>> map = new TreeMap<>();

    public <O extends Enum<O> & Operation<O>> void compute(Class<O> c, Pattern p, Set<O> operation) {
        map.compute(new Key(c, p), (key, value) -> {
            if (value == null) {
                return EnumSet.copyOf(operation);
            }
            value.addAll((Set) operation);
            return value;
        });
    }

    public <O extends Enum<O> & Operation<O>> Set<O> lookup(Class<O> c, String name) {
        var submap = map.tailMap(new Key(c, Pattern.compile("")));
        for (var entry: submap.entrySet()) {
            if (!entry.getKey().c().equals(c)) {
                break;
            }
            if (entry.getKey().p().matcher(name).matches()) {
                return (Set) entry.getValue();
            }
        }
        return null;
    }
}
