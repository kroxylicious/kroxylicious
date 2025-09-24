/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization.subject;

public interface ClientSubjectBuilderService<C> {
    void initialize(C config);

    ClientSubjectBuilder build();

    default void close() { }
}
