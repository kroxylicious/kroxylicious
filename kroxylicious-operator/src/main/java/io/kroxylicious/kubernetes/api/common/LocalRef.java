/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import java.util.Objects;

/**
 * Abstraction for references in one kubernetes resource to some kubernetes resource in the same namespace.
 * Two LocalRefs are equal iff they have the same group, kind and name (they don't need to have the same class)
 * @param <T> The Java type of the resource
 */
public abstract class LocalRef<T> {

    public abstract String getGroup();

    public abstract String getKind();

    public abstract String getName();

    @Override
    public final int hashCode() {
        return Objects.hash(getGroup(), getKind(), getName());
    }

    @Override
    @SuppressWarnings("unchecked")
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LocalRef)) {
            return false;
        }
        LocalRef<T> other = (LocalRef<T>) obj;
        return Objects.equals(getGroup(), other.getGroup())
                && Objects.equals(getKind(), other.getKind())
                && Objects.equals(getName(), other.getName());
    }
}
