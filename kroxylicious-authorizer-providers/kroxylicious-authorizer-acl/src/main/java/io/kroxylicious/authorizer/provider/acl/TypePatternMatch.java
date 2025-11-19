/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.EnumSet;
import java.util.Set;
import java.util.TreeMap;

import com.google.re2j.Pattern;

import io.kroxylicious.authorizer.service.ResourceType;

import edu.umd.cs.findbugs.annotations.Nullable;

class TypePatternMatch {

    record Key(
               Class<?> c,
               Pattern p)
            implements Comparable<TypePatternMatch.Key> {

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

    public <O extends Enum<O> & ResourceType<O>> void compute(Class<O> c, Pattern p, Set<O> operation) {
        map.compute(new Key(c, p), (key, value) -> {
            if (value == null) {
                return EnumSet.copyOf(operation);
            }
            value.addAll((Set) operation);
            return value;
        });
    }

    public <O extends Enum<O> & ResourceType<O>> @Nullable Set<O> lookup(Class<O> c, String name) {
        EnumSet<O> result = null;
        var submap = map.tailMap(new Key(c, Pattern.compile("")));
        for (var entry : submap.entrySet()) {
            if (!entry.getKey().c().equals(c)) {
                break;
            }
            if (entry.getKey().p().matcher(name).matches()) {
                if (result == null) {
                    result = EnumSet.noneOf(c);
                }
                result.addAll((Set) entry.getValue());
            }
        }
        return result;
    }
}
