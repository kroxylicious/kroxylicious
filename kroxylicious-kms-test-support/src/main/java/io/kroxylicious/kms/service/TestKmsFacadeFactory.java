/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Factory for {@link TestKmsFacade}s.
 * <br/>
 * The {@code KROXYLICIOUS_KMS_FACADE_CLASS_NAME_FILTER} environment variable is a regular expression
 * that limits the facades available to tests. It matches against the facade class name.
 * If not set, all available facades are used.
 *
 * @param <C> The config type
 * @param <K> The key reference
 * @param <E> The type of encrypted DEK
 */
public interface TestKmsFacadeFactory<C, K, E> {

    /**
     * Pattern used to filter facades based on the {@code KROXYLICIOUS_KMS_FACADE_CLASS_NAME_FILTER} environment variable.
     * Defaults to matching all class names if the environment variable is not set.
     */
    Pattern FACADE_CLASS_NAME_FILTER = Optional.ofNullable(System.getenv("KROXYLICIOUS_KMS_FACADE_CLASS_NAME_FILTER"))
            .map(Pattern::compile)
            .orElse(Pattern.compile(".*"));

    /**
     * Creates a TestKmsFacade instance
     *
     * @return instance
     */
    TestKmsFacade<C, K, E> build();

    /**
     * Discovers the available {@link TestKmsFacadeFactory}.
     *
     * @return factories
     */
    @SuppressWarnings("unchecked")
    static <C, K, E> Stream<TestKmsFacadeFactory<C, K, E>> getTestKmsFacadeFactories() {
        return ServiceLoader.load(TestKmsFacadeFactory.class).stream()
                .map(ServiceLoader.Provider::get);
    }

    /**
     * Discovers and builds available {@link TestKmsFacade}s, filtered by the
     * {@code KROXYLICIOUS_KMS_FACADE_CLASS_NAME_FILTER} environment variable and availability.
     *
     * @return stream of available test KMS facades
     */
    static Stream<? extends TestKmsFacade<?, ?, ?>> getAvailableTestKmsFacades() {
        return getTestKmsFacadeFactories()
                .map(TestKmsFacadeFactory::build)
                .filter(f -> FACADE_CLASS_NAME_FILTER.matcher(f.getClass().getName()).matches())
                .filter(TestKmsFacade::isAvailable);
    }
}
