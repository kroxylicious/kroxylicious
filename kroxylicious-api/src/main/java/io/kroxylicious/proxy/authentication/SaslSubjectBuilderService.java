/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

/**
 * The service interface used to construct a {@link SaslSubjectBuilder}.
 *
 * @param <C> The configuration type consumed by the particular {@link SaslSubjectBuilder} implementation.
 */
public interface SaslSubjectBuilderService<C> extends AutoCloseable {
    /**
     * Initializes this service with its configuration.
     * @param config The service configuration.
     */
    void initialize(C config);

    /**
     * Builds a {@link SaslSubjectBuilder}.
     * @return The builder.
     */
    SaslSubjectBuilder build();

    @Override
    default void close() {
    }
}
