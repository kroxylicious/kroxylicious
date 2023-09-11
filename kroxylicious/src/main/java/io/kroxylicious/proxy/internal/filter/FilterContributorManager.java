/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterConstructContext;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.service.ContributionManager;

public class FilterContributorManager {

    private static final FilterContributorManager INSTANCE = new FilterContributorManager();
    private final ContributionManager<Filter, FilterConstructContext, FilterContributor> contributionManager;

    private FilterContributorManager() {
        contributionManager = new ContributionManager<>();
    }

    public static FilterContributorManager getInstance() {
        return INSTANCE;
    }

    public Class<? extends BaseConfig> getConfigType(String shortName) {
        return contributionManager.getDefinition(FilterContributor.class, shortName).configurationType();
    }

    public Filter getFilter(String shortName, NettyFilterContext context, BaseConfig filterConfig) {
        return contributionManager.getInstance(FilterContributor.class, shortName, context.wrap(filterConfig));
    }
}