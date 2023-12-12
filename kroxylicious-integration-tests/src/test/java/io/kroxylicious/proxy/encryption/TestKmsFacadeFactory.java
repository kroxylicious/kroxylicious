/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.encryption;

import java.util.ServiceLoader;
import java.util.stream.Stream;

/**
 * Factory for {@link TestKmsFacade}s.
 */
public interface TestKmsFacadeFactory {

    /**
     * Creates a TestKmsFacade instance
     *
     * @return instance
     */
    TestKmsFacade build();

    /**
     * Discovers the available {@link TestKmsFacadeFactory}.
     * @return factories
     */
    static Stream<TestKmsFacadeFactory> getTestKmsFacadeFactories() {
        return ServiceLoader.load(TestKmsFacadeFactory.class).stream()
                .map(ServiceLoader.Provider::get);
    }
}
