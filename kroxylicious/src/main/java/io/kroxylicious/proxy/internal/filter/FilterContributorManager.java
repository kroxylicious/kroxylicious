/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.config.AbstractContributorManager;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.filter.KrpcFilter;

public class FilterContributorManager extends AbstractContributorManager<FilterContributor, KrpcFilter> {

    private static final FilterContributorManager INSTANCE = new FilterContributorManager();

    private FilterContributorManager() {
        super(FilterContributor.class);
    }

    public static FilterContributorManager getInstance() {
        return INSTANCE;
    }

}
