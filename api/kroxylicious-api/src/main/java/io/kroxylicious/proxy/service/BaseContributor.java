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
import io.kroxylicious.proxy.config.ProxyConfig;

/**
 * A convenience base class for creating concrete contributor subclasses using a typesafe builder
 */
public abstract class BaseContributor<L> implements Contributor<L> {

    private final Map<String, InstanceBuilder<? extends BaseConfig, L>> shortNameToInstanceBuilder;

    public BaseContributor(BaseContributorBuilder<L> builder) {
        shortNameToInstanceBuilder = builder.build();
    }

    @Override
    public Class<? extends BaseConfig> getConfigType(String shortName) {
        InstanceBuilder<?, L> instanceBuilder = shortNameToInstanceBuilder.get(shortName);
        return instanceBuilder == null ? null : instanceBuilder.configClass;
    }

    @Override
    public L getInstance(String shortName, ProxyConfig proxyConfig, BaseConfig config) {
        InstanceBuilder<? extends BaseConfig, L> instanceBuilder = shortNameToInstanceBuilder.get(shortName);
        return instanceBuilder == null ? null : instanceBuilder.construct(proxyConfig, config);
    }

    private static class InstanceBuilder<T extends BaseConfig, L> {

        private final Class<T> configClass;
        private final BiFunction<ProxyConfig, T, L> instanceFunction;

        InstanceBuilder(Class<T> configClass, BiFunction<ProxyConfig, T, L> instanceFunction) {
            this.configClass = configClass;
            this.instanceFunction = instanceFunction;
        }

        L construct(ProxyConfig proxyConfig, BaseConfig config) {
            if (config == null) {
                // tests pass in a null config, which some instance functions can tolerate
                return instanceFunction.apply(proxyConfig, null);
            }
            else if (configClass.isAssignableFrom(config.getClass())) {
                return instanceFunction.apply(proxyConfig, configClass.cast(config));
            }
            else {
                throw new IllegalArgumentException("config has the wrong type, expected "
                        + configClass.getName() + ", got " + config.getClass().getName());
            }
        }
    }

    public static class BaseContributorBuilder<L> {

        private BaseContributorBuilder() {
        }

        private final Map<String, InstanceBuilder<?, L>> shortNameToInstanceBuilder = new HashMap<>();

        public <T extends BaseConfig> BaseContributorBuilder<L> add(String shortName, Class<T> configClass, BiFunction<ProxyConfig, T, L> instanceFunction) {
            if (shortNameToInstanceBuilder.containsKey(shortName)) {
                throw new IllegalArgumentException(shortName + " already registered");
            }
            shortNameToInstanceBuilder.put(shortName, new InstanceBuilder<>(configClass, instanceFunction));
            return this;
        }

        public BaseContributorBuilder<L> add(String shortName, Function<ProxyConfig, L> instanceFunction) {
            add(shortName, BaseConfig.class, (proxyConfig, config) -> instanceFunction.apply(proxyConfig));
            return this;
        }

        public <T extends BaseConfig> BaseContributorBuilder<L> add(String shortName, Class<T> configClass, Function<T, L> instanceFunction) {
            add(shortName, configClass, (proxyConfig, config) -> instanceFunction.apply(config));
            return this;
        }

        public BaseContributorBuilder<L> add(String shortName, Supplier<L> instanceFunction) {
            add(shortName, BaseConfig.class, (proxyConfig, config) -> instanceFunction.get());
            return this;
        }

        public Map<String, InstanceBuilder<?, L>> build() {
            return Map.copyOf(shortNameToInstanceBuilder);
        }
    }

    public static <L> BaseContributorBuilder<L> builder() {
        return new BaseContributorBuilder<>();
    }
}
