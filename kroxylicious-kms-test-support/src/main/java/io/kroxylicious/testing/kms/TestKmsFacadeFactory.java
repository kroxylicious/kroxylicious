/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Factory for {@link TestKmsFacade}s.
 * <br/>
 * The {@code KROXYLICIOUS_KMS_FACADE_FACTORY_CLASS_NAME_FILTER} environment variable is a regular expression
 * that limits the facades available to tests. It matches against the facade factory class name.
 * If not set, all available facades are used.
 *
 * @param <C> The config type
 * @param <K> The key reference
 * @param <E> The type of encrypted DEK
 */
public interface TestKmsFacadeFactory<C, K, E> {

    /** Name of the environment variable used for facade factory filtering. */
    String FACADE_FACTORY_CLASS_NAME_FILTER_ENV_VAR = "KROXYLICIOUS_KMS_FACADE_FACTORY_CLASS_NAME_FILTER";
    /**
     * Pattern used to filter facade factories based on the {@code KROXYLICIOUS_KMS_FACADE_FACTORY_CLASS_NAME_FILTER} environment variable.
     * Defaults to matching all class names if the environment variable is not set.
     */
    Pattern FACADE_FACTORY_CLASS_NAME_FILTER = Optional.ofNullable(System.getenv(FACADE_FACTORY_CLASS_NAME_FILTER_ENV_VAR))
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
        return (Stream<TestKmsFacadeFactory<C, K, E>>) (Stream<?>) ServiceLoader.load(TestKmsFacadeFactory.class).stream()
                .map(ServiceLoader.Provider::get)
                .filter(f -> FACADE_FACTORY_CLASS_NAME_FILTER.matcher(f.getClass().getName()).matches());
    }

}
