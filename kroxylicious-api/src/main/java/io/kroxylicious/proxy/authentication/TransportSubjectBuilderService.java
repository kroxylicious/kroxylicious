/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

/**
 * The service interface used to construct a {@link TransportSubjectBuilder}.
 * A {@code TransportSubjectBuilderService} can be specified on a virtual cluster.
 * 
 * @param <C> The configuration type consumed by the particular {@link TransportSubjectBuilder} implementation.
 */
public interface TransportSubjectBuilderService<C> extends AutoCloseable {
    void initializeTransportSubjectBuilderService(C config);

    TransportSubjectBuilder buildTransportSubjectBuilderService();

    default void close() {
    }
}
