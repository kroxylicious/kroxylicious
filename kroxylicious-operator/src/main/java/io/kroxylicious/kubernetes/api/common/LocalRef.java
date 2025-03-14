/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

/**
 * Abstraction for references in one kubernetes resource to some kubernetes resource in the same namespace
 * @param <T> The Java type of the resource
 */
public interface LocalRef<T> {

    public String getGroup();

    public String getKind();

    public String getName();

}
