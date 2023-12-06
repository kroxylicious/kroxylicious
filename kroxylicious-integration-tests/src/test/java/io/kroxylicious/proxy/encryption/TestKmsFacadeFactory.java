/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.encryption;

import java.util.ServiceLoader;
import java.util.stream.Stream;

public interface TestKmsFacadeFactory {

    /**
     * Returns true of the TestKmsFacade is available, or false otherwise.
     * @return true if available, false otherwise.
     */
    default boolean isAvailable() {
        return true;
    }

    /**
     * Creates a TestKmsFacade instance
     *
     * @return instance
     */
    TestKmsFacade build();

    static Stream<TestKmsFacadeFactory> getTestKmsFacadeFactories() {
        return ServiceLoader.load(TestKmsFacadeFactory.class).stream()
                .map(ServiceLoader.Provider::get);
    }
}
