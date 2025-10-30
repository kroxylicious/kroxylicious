/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;

public interface TransportSubjectBuilderService<C> extends AutoCloseable {
    void initializeTransportSubjectBuilderService(C config);

    TransportSubjectBuilder buildTransportSubjectBuilderService();

    default void close() {
    }
}
