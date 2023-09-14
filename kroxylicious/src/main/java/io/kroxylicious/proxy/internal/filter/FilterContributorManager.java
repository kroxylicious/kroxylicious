/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import java.util.ServiceLoader;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterContributor;

public class FilterContributorManager {

    private static final FilterContributorManager INSTANCE = new FilterContributorManager();

    private final ServiceLoader<FilterContributor> contributors;

    private FilterContributorManager() {
        this.contributors = ServiceLoader.load(FilterContributor.class);
    }

    public static FilterContributorManager getInstance() {
        return INSTANCE;
    }

    public Filter getFilter(String shortName, NettyFilterContext context, BaseConfig filterConfig) {
        return getContributor(shortName).getInstance(filterConfig, context);
    }

    public Class<? extends BaseConfig> getConfigType(String shortName) {
        return getContributor(shortName).getConfigClass();
    }

    public FilterContributor getContributor(String shortName) {
        return contributors.stream().map(ServiceLoader.Provider::get)
                .filter(filterContributor -> filterContributor.getTypeName().equals(shortName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No filter found for name '" + shortName + "'"));
    }
}
