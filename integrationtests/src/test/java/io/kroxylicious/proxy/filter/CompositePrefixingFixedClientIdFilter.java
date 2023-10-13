/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.List;

public class CompositePrefixingFixedClientIdFilter implements CompositeFilter {

    private final CompositePrefixingFixedClientIdFilterConfig config;

    public CompositePrefixingFixedClientIdFilter(CompositePrefixingFixedClientIdFilterConfig config) {
        this.config = config;
    }

    @Override
    public List<Filter> getFilters() {
        FixedClientIdFilter clientIdFilter = new FixedClientIdFilter(new FixedClientIdFilter.FixedClientIdFilterConfig(config().clientId()));
        return List.of(clientIdFilter, new CompositePrefixingFixedClientIdFilterFactory.PrefixingFilter(this));
    }

    public CompositePrefixingFixedClientIdFilterConfig config() {
        return config;
    }
}
