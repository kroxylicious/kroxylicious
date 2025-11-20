/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.Objects;

import com.google.re2j.Pattern;

record ResourceMatcherNameMatches<T>(Class<T> type, Pattern pattern) implements Key<T>, Comparable<ResourceMatcherNameMatches<T>> {

    ResourceMatcherNameMatches {
        Objects.requireNonNull(type);
        Objects.requireNonNull(pattern);
    }

    @Override
    public int compareTo(ResourceMatcherNameMatches<T> o) {
        var cmp = type.getName().compareTo(o.type.getName());
        if (cmp == 0) {
            cmp = pattern.pattern().compareTo(o.pattern.pattern());
        }
        return cmp;
    }

}
