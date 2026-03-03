/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

import edu.umd.cs.findbugs.annotations.UnknownNullness;

/**
 * The service interface used to construct a {@link TransportSubjectBuilder}.
 *
 * @param <C> The configuration type consumed by the particular {@link TransportSubjectBuilder} implementation.
 */
public interface TransportSubjectBuilderService<C> extends AutoCloseable {
    void initialize(@UnknownNullness C config);

    TransportSubjectBuilder build();

    default void close() {
    }
}
