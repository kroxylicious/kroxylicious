/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import java.util.Optional;
import java.util.ServiceLoader;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.service.InstanceFactory;

public class FilterContributorManager {

    private static final FilterContributorManager INSTANCE = new FilterContributorManager();

    private final ServiceLoader<FilterContributor> contributors;

    private FilterContributorManager() {
        this.contributors = ServiceLoader.load(FilterContributor.class);
    }

    public static FilterContributorManager getInstance() {
        return INSTANCE;
    }

    public Class<? extends BaseConfig> getConfigType(String shortName) {
        for (FilterContributor contributor : contributors) {
            Optional<InstanceFactory<Filter>> configType = contributor.getInstanceFactory(shortName);
            if (configType.isPresent()) {
                return configType.get().getConfigClass();
            }
        }

        throw new IllegalArgumentException("No filter found for name '" + shortName + "'");
    }

    public Filter getFilter(String shortName, BaseConfig filterConfig) {
        for (FilterContributor contributor : contributors) {
            Optional<InstanceFactory<Filter>> configType = contributor.getInstanceFactory(shortName);
            if (configType.isPresent()) {
                return configType.get().getInstance(filterConfig);
            }
        }

        throw new IllegalArgumentException("No filter found for name '" + shortName + "'");
    }
}
