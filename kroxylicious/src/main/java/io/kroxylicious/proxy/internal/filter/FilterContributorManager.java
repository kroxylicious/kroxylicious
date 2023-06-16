/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import java.util.List;
import java.util.ServiceLoader;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.filter.KrpcFilter;

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
            Class<? extends BaseConfig> configType = contributor.getConfigType(shortName);
            if (configType != null) {
                return configType;
            }
        }

        throw new IllegalArgumentException("No filters found for name '" + shortName + "'");
    }

    public List<KrpcFilter> getFilters(String shortName, BaseConfig filterConfig) {
        for (FilterContributor contributor : contributors) {
            List<KrpcFilter> filters = contributor.getInstance(shortName, filterConfig);
            if (filters != null) {
                return filters;
            }
        }

        throw new IllegalArgumentException("No filters found for name '" + shortName + "'");
    }
}
