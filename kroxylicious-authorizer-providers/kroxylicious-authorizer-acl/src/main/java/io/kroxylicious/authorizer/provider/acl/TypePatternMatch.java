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

    private static final Pattern LOWER_BOUND = Pattern.compile("");
    private final TreeMap<ResourceMatcherNameMatches<?>, EnumSet<?>> map = new TreeMap<>();

    public <O extends Enum<O> & ResourceType<O>> void add(ResourceMatcherNameMatches<?> key, Set<O> operation) {
        map.compute(key, (key1, value) -> {
            if (value == null) {
                return EnumSet.copyOf(operation);
            }
            value.addAll((Set) operation);
            return value;
        });
    }

    public @Nullable <T extends Enum<T> & ResourceType<T>> Set<T> matchingOperations(Class<T> c, String name) {
        EnumSet<T> result = null;
        var submap = map.tailMap(new ResourceMatcherNameMatches<>(c, LOWER_BOUND));
        for (var entry : submap.entrySet()) {
            if (!entry.getKey().type().equals(c)) {
                break;
            }
            if (entry.getKey().pattern().matcher(name).matches()) {
                if (result == null) {
                    result = EnumSet.noneOf(c);
                }
                result.addAll((Set) entry.getValue());
            }
        }
        return result;
    }
}
