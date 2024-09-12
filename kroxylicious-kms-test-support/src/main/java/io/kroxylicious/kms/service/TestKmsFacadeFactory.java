/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import java.util.ServiceLoader;
import java.util.stream.Stream;

/**
 * Factory for {@link TestKmsFacade}s.
 * @param <C> The config type
 * @param <K> The key reference
 * @param <E> The type of encrypted DEK
 */
public interface TestKmsFacadeFactory<C, K, E> {

    /**
     * Creates a TestKmsFacade instance
     *
     * @return instance
     */
    TestKmsFacade<C, K, E> build();

    /**
     * Discovers the available {@link TestKmsFacadeFactory}.
     * @return factories
     */
    @SuppressWarnings(
        "unchecked"
    )
    static <C, K, E> Stream<TestKmsFacadeFactory<C, K, E>> getTestKmsFacadeFactories() {
        return ServiceLoader.load(TestKmsFacadeFactory.class)
                            .stream()
                            .map(ServiceLoader.Provider::get);
    }
}
