/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import io.kroxylicious.proxy.config.BaseConfig;

/**
 * A convenience base class for creating concrete contributor subclasses using a typesafe builder
 *
 * @param <T> the service type
 */
public abstract class BaseContributor<T, S extends Context> implements Contributor<T, S> {

    private final Map<String, ContributionDetails<T, S>> typeNameToInstanceBuilder;

    /**
     * Constructs and configures the contributor using the supplied {@code builder}.
     * @param builder builder
     */
    protected BaseContributor(BaseContributorBuilder<T, S> builder) {
        typeNameToInstanceBuilder = builder.build();
    }

    @Override
    public boolean contributes(String typeName) {
        return typeNameToInstanceBuilder.containsKey(typeName);
    }

    @Override
    public ConfigurationDefinition getConfigDefinition(String typeName) {
        if (typeNameToInstanceBuilder.containsKey(typeName)) {
            return typeNameToInstanceBuilder.get(typeName).configurationDefinition();
        }
        else {
            throw new IllegalArgumentException("No configuration definition registered for " + typeName);
        }
    }

    @SuppressWarnings("removal")
    @Override
    public Class<? extends BaseConfig> getConfigType(String typeName) {
        if (typeNameToInstanceBuilder.containsKey(typeName)) {
            return typeNameToInstanceBuilder.get(typeName).configurationDefinition().configurationType();
        }
        else {
            return null;
        }
    }

    @Override
    public T getInstance(String typeName, S context) {
        if (typeNameToInstanceBuilder.containsKey(typeName)) {
            InstanceBuilder<T, S> instanceBuilder = typeNameToInstanceBuilder.get(typeName).instanceBuilder();
            return instanceBuilder == null ? null : instanceBuilder.construct(context);
        }
        else {
            return null;
        }
    }

    private static class InstanceBuilder<L, D extends Context> {

        private final Function<D, L> instanceFunction;

        InstanceBuilder(Function<D, L> instanceFunction) {
            this.instanceFunction = instanceFunction;
        }

        L construct(D context) {
            return instanceFunction.apply(context);
        }

        static <T extends BaseConfig, L, D extends Context> InstanceBuilder<L, D> builder(Class<T> configClass, BiFunction<D, T, L> instanceFunction) {
            return new InstanceBuilder<>(context -> {
                BaseConfig config = context.getConfig();
                if (config == null) {
                    // tests pass in a null config, which some instance functions can tolerate
                    return instanceFunction.apply(context, null);
                }
                else if (configClass.isAssignableFrom(config.getClass())) {
                    return instanceFunction.apply(context, configClass.cast(config));
                }
                else {
                    throw new IllegalArgumentException("config has the wrong type, expected "
                            + configClass.getName() + ", got " + config.getClass().getName());
                }
            });

        }
    }

    /**
     * Builder for the registration of contributor service implementations.
     * @see BaseContributor#builder()
     * @param <L> the service type
     */
    public static class BaseContributorBuilder<L, D extends Context> {

        private BaseContributorBuilder() {
        }

        private final Map<String, ContributionDetails<L, D>> typeNameToInstanceBuilder = new HashMap<>();

        /**
         * Registers a factory function for the construction of a service instance.
         *
         * @param typeName service short name
         * @param configClass concrete type of configuration required by the service
         * @param instanceFunction function that constructs the service instance
         * @return this
         * @param <T> the configuration concrete type
         */
        public <T extends BaseConfig> BaseContributorBuilder<L, D> add(String typeName, Class<T> configClass, Function<T, L> instanceFunction) {
            return add(typeName, configClass, (context, config) -> instanceFunction.apply(config));
        }

        /**
         * Registers a factory function for the construction of a service instance.
         *
         * @param typeName service short name
         * @param configClass concrete type of configuration required by the service
         * @param instanceFunction function that constructs the service instance
         * @return this
         * @param <T> the configuration concrete type
         */
        public <T extends BaseConfig> BaseContributorBuilder<L, D> add(String typeName, Class<T> configClass, BiFunction<D, T, L> instanceFunction) {
            if (typeNameToInstanceBuilder.containsKey(typeName)) {
                throw new IllegalArgumentException(typeName + " already registered");
            }
            typeNameToInstanceBuilder.put(typeName,
                    new ContributionDetails<>(new ConfigurationDefinition(configClass), InstanceBuilder.builder(configClass, instanceFunction)));
            return this;
        }

        /**
         * Registers a factory function for the construction of a service instance.
         *
         * @param typeName service short name
         * @param instanceFunction function that constructs the service instance
         * @return this
         */
        public BaseContributorBuilder<L, D> add(String typeName, Supplier<L> instanceFunction) {
            return add(typeName, BaseConfig.class, config -> instanceFunction.get());
        }

        /**
         * Registers a factory function for the construction of a service instance.
         *
         * @param typeName service short name
         * @param instanceFunction function that constructs the service instance from a context
         * @return this
         */
        public BaseContributorBuilder<L, D> add(String typeName, Function<D, L> instanceFunction) {
            return add(typeName, BaseConfig.class, (context, config) -> instanceFunction.apply(context));
        }

        Map<String, ContributionDetails<L, D>> build() {
            return Map.copyOf(typeNameToInstanceBuilder);
        }
    }

    /**
     * Creates a builder for the registration of contributor service implementations.
     *
     * @return the builder
     * @param <L> the service type
     */
    public static <L, D extends Context> BaseContributorBuilder<L, D> builder() {
        return new BaseContributorBuilder<>();
    }

    protected record ContributionDetails<T, D extends Context>(ConfigurationDefinition configurationDefinition, InstanceBuilder<T, D> instanceBuilder) {
    }
}