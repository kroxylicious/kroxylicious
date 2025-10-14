/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

public interface SaslSubjectBuilderService<C> extends AutoCloseable {
    void initializeSaslSubjectBuilderService(C config);

    SaslSubjectBuilder buildSaslSubjectBuilderService();

    default void close() {
    }
}
