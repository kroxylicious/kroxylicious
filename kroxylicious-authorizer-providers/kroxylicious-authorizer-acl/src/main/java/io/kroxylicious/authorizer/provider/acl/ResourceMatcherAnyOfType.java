/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.Objects;

import edu.umd.cs.findbugs.annotations.Nullable;

record ResourceMatcherAnyOfType<T>(Class<? extends T> type) implements OrderedKey<T> {

    ResourceMatcherAnyOfType {
        Objects.requireNonNull(type);
    }

    @Nullable
    @Override
    public String operand() {
        return null;
    }
}
