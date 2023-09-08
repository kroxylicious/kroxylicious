/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.internal.ContributorManager;

public class FilterContributorManager extends ContributorManager<Filter, FilterContributor> {

    private static final FilterContributorManager INSTANCE = new FilterContributorManager();

    private FilterContributorManager() {
        super(FilterContributor.class, "filter");
    }

    public static FilterContributorManager getInstance() {
        return INSTANCE;
    }
}
