/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Optional;
import java.util.ServiceLoader;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.Contributor;
import io.kroxylicious.proxy.service.SpecificContributor;

public class ContributorManager<Y, T extends Contributor<Y>> {

    private final ServiceLoader<T> contributors;
    private final String type;

    public ContributorManager(Class<T> serviceClass, String type) {
        this.contributors = ServiceLoader.load(serviceClass);
        this.type = type;
    }

    public Class<? extends BaseConfig> getConfigType(String shortName) {
        for (T contributor : contributors) {
            Optional<SpecificContributor<Y>> configType = contributor.getSpecificContributor(shortName);
            if (configType.isPresent()) {
                return configType.get().getConfigClass();
            }
        }

        throw new IllegalArgumentException("No " + type + " found for name '" + shortName + "'");
    }

    public Y getInstance(String shortName, BaseConfig config) {
        for (T contributor : contributors) {
            Optional<SpecificContributor<Y>> configType = contributor.getSpecificContributor(shortName);
            if (configType.isPresent()) {
                return configType.get().getInstance(config);
            }
        }

        throw new IllegalArgumentException("No " + type + " found for name '" + shortName + "'");
    }
}
