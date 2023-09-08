/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import io.kroxylicious.proxy.config.BaseConfig;

/**
 * A convenience base class for creating concrete contributor subclasses using a typesafe builder
 *
 * @param <T> the service type
 */
public abstract class BaseContributor<T> implements Contributor<T> {

    private final Map<String, ContributorDetails<? extends BaseConfig, T>> shortNameToContributorDetails;

    /**
     * Constructs and configures the contributor using the supplied {@code builder}.
     *
     * @param builder builder
     */
    protected BaseContributor(BaseContributorBuilder<T> builder) {
        shortNameToContributorDetails = builder.build();
    }

    @Override
    public Class<? extends BaseConfig> getConfigType(String shortName) {
        if (shortNameToContributorDetails.containsKey(shortName)) {
            InstanceBuilder<?, T> instanceBuilder = shortNameToContributorDetails.get(shortName).instanceBuilder();
            return instanceBuilder == null ? null : instanceBuilder.configClass;
        }
        else {
            return null;
        }
    }

    @Override
    @SuppressWarnings("java:S2447")
    public Boolean requiresConfig(String shortName) {
        if (shortNameToContributorDetails.containsKey(shortName)) {
            return shortNameToContributorDetails.get(shortName).configRequired();
        }
        else {
            return null;
        }
    }

    @Override
    public T getInstance(String shortName, BaseConfig config) {
        if (shortNameToContributorDetails.containsKey(shortName)) {
            final ContributorDetails<?, T> contributorDetails = shortNameToContributorDetails.get(shortName);
            InstanceBuilder<? extends BaseConfig, T> instanceBuilder = contributorDetails.instanceBuilder;
            if (contributorDetails.configRequired() && Objects.isNull(config)) {
                throw new IllegalArgumentException(shortName + " requires config but it is null");
            }
            return instanceBuilder == null ? null : instanceBuilder.construct(config);
        }
        else {
            return null;
        }
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
     *
     * @param <L> the service type
     * @see BaseContributor#builder()
     */
    public static class BaseContributorBuilder<L> {

        private BaseContributorBuilder() {
        }

        private final Map<String, ContributorDetails<? extends BaseConfig, L>> shortNameToInstanceBuilder = new HashMap<>();

        /**
         * Registers a factory function for the construction of a service instance.
         *
         * @param shortName        service short name
         * @param configClass      concrete type of configuration required by the service
         * @param instanceFunction function that constructs the service instance
         * @param <T>              the configuration concrete type
         * @return this
         */
        public <T extends BaseConfig> BaseContributorBuilder<L> add(String shortName, Class<T> configClass, Function<T, L> instanceFunction) {
            return add(shortName, configClass, instanceFunction, !Objects.equals(BaseConfig.class, configClass));
        }

        /**
         * Registers a factory function for the construction of a service instance.
         *
         * @param shortName        service short name
         * @param configClass      concrete type of configuration required by the service
         * @param instanceFunction function that constructs the service instance
         * @param <T>              the configuration concrete type
         * @return this
         */
        public <T extends BaseConfig> BaseContributorBuilder<L> add(String shortName, Class<T> configClass, Function<T, L> instanceFunction, boolean configRequired) {
            if (shortNameToInstanceBuilder.containsKey(shortName)) {
                throw new IllegalArgumentException(shortName + " already registered");
            }
            shortNameToInstanceBuilder.put(shortName, new ContributorDetails<>(new InstanceBuilder<>(configClass, instanceFunction), configRequired));
            return this;
        }

        /**
         * Registers a factory function for the construction of a service instance.
         *
         * @param shortName        service short name
         * @param instanceFunction function that constructs the service instance
         * @return this
         */
        public BaseContributorBuilder<L> add(String shortName, Supplier<L> instanceFunction) {
            return add(shortName, BaseConfig.class, config -> instanceFunction.get());
        }

        @SuppressWarnings("java:S1452")
        Map<String, ContributorDetails<? extends BaseConfig, L>> build() {
            return Map.copyOf(shortNameToInstanceBuilder);
        }
    }

    /**
     * Creates a builder for the registration of contributor service implementations.
     *
     * @param <L> the service type
     * @return the builder
     */
    public static <L> BaseContributorBuilder<L> builder() {
        return new BaseContributorBuilder<>();
    }

    private record ContributorDetails<C extends BaseConfig, T>(InstanceBuilder<C, T> instanceBuilder, boolean configRequired) {}
}