/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import io.kroxylicious.proxy.config.BaseConfig;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A convenience base class for creating concrete contributor subclasses using a typesafe builder
 *
 * @param <T> the service type
 */
public abstract class BaseContributor<T> implements Contributor<T> {

    private final Map<String, InstanceBuilder<? extends BaseConfig, T>> shortNameToInstanceBuilder;

    /**
     * Constructs and configures the contributor using the supplied {@code builder}.
     * @param builder builder
     */
    public BaseContributor(BaseContributorBuilder<T> builder) {
        shortNameToInstanceBuilder = builder.build();
    }

    @NonNull
    @Override
    public Optional<InstanceFactory<T>> getInstanceFactory(String shortName) {
        return Optional.ofNullable(shortNameToInstanceBuilder.get(shortName)).map(tInstanceBuilder -> new InstanceFactory<T>() {
            @Override
            public Class<? extends BaseConfig> getConfigClass() {
                return tInstanceBuilder.configClass;
            }

            @Override
            public T getInstance(BaseConfig config) {
                return tInstanceBuilder.construct(config);
            }
        });
    }

    private static class InstanceBuilder<T extends BaseConfig, L> {

        private final Class<T> configClass;
        private final Function<T, L> instanceFunction;

        InstanceBuilder(Class<T> configClass, Function<T, L> instanceFunction) {
            this.configClass = configClass;
            this.instanceFunction = instanceFunction;
        }

        L construct(BaseConfig config) {
            if (config == null) {
                // tests pass in a null config, which some instance functions can tolerate
                return instanceFunction.apply(null);
            }
            else if (configClass.isAssignableFrom(config.getClass())) {
                return instanceFunction.apply(configClass.cast(config));
            }
            else {
                throw new IllegalArgumentException("config has the wrong type, expected "
                        + configClass.getName() + ", got " + config.getClass().getName());
            }
        }
    }

    /**
     * Builder for the registration of contributor service implementations.
     * @see BaseContributor#builder()
     * @param <L> the service type
     */
    public static class BaseContributorBuilder<L> {

        private BaseContributorBuilder() {
        }

        private final Map<String, InstanceBuilder<?, L>> shortNameToInstanceBuilder = new HashMap<>();

        /**
         * Registers a factory function for the construction of a service instance.
         *
         * @param shortName service short name
         * @param configClass concrete type of configuration required by the service
         * @param instanceFunction function that constructs the service instance
         * @return this
         * @param <T> the configuration concrete type
         */
        public <T extends BaseConfig> BaseContributorBuilder<L> add(String shortName, Class<T> configClass, Function<T, L> instanceFunction) {
            if (shortNameToInstanceBuilder.containsKey(shortName)) {
                throw new IllegalArgumentException(shortName + " already registered");
            }
            shortNameToInstanceBuilder.put(shortName, new InstanceBuilder<>(configClass, instanceFunction));
            return this;
        }

        /**
         * Registers a factory function for the construction of a service instance.
         *
         * @param shortName service short name
         * @param instanceFunction function that constructs the service instance
         * @return this
         */
        public BaseContributorBuilder<L> add(String shortName, Supplier<L> instanceFunction) {
            return add(shortName, BaseConfig.class, (config) -> instanceFunction.get());
        }

        Map<String, InstanceBuilder<?, L>> build() {
            return Map.copyOf(shortNameToInstanceBuilder);
        }
    }

    /**
     * Creates a builder for the registration of contributor service implementations.
     *
     * @return the builder
     * @param <L> the service type
     */
    public static <L> BaseContributorBuilder<L> builder() {
        return new BaseContributorBuilder<>();
    }
}
