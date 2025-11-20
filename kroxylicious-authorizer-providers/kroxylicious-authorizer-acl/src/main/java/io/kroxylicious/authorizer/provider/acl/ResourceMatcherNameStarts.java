/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.Objects;

import edu.umd.cs.findbugs.annotations.NonNull;

record ResourceMatcherNameStarts<T>(Class<? extends T> type, String prefix) implements OrderedKey<T> {

    ResourceMatcherNameStarts {
        Objects.requireNonNull(type);
        Objects.requireNonNull(prefix);
    }

    @NonNull
    @Override
    public String operand() {
        return prefix;
    }
}
