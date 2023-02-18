/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.ServiceLoader;

import io.kroxylicious.proxy.service.Contributor;

public class AbstractContributorManager<S extends Contributor<T>, T> {
    private final ServiceLoader<S> contributors;

    public AbstractContributorManager(Class<S> clazz) {
        this.contributors = ServiceLoader.load(clazz);
    }

    public Class<? extends BaseConfig> getConfigType(String shortName) {
        for (S contributor : contributors) {
            Class<? extends BaseConfig> configType = contributor.getConfigType(shortName);
            if (configType != null) {
                return configType;
            }
        }

        throw typeNotFound(shortName);
    }

    public T getInstance(String shortName, ProxyConfig proxyConfig, BaseConfig config) {
        for (S contributor : contributors) {
            T instance = contributor.getInstance(shortName, proxyConfig, config);
            if (instance != null) {
                return instance;
            }
        }

        throw typeNotFound(shortName);
    }

    private IllegalArgumentException typeNotFound(String shortName) {
        return new IllegalArgumentException(String.format("No %s contributor provides type for name '%s'", this.getClass().getSimpleName(), shortName));
    }
}
