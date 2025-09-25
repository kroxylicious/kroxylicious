/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

public interface SubjectBuilderService<C> {
    void initialize(C config);

    SubjectBuilder build();

    default void close() {
    }
}
