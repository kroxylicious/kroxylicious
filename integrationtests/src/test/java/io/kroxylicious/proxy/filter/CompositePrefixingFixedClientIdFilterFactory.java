/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.filter.CompositePrefixingFixedClientIdFilter.CompositePrefixingFixedClientIdFilterConfig;

public class CompositePrefixingFixedClientIdFilterFactory
        implements FilterFactory<CompositePrefixingFixedClientIdFilter, CompositePrefixingFixedClientIdFilterConfig> {

    @Override
    public Class<CompositePrefixingFixedClientIdFilter> filterType() {
        return CompositePrefixingFixedClientIdFilter.class;
    }

    @Override
    public Class<CompositePrefixingFixedClientIdFilterConfig> configType() {
        return CompositePrefixingFixedClientIdFilterConfig.class;
    }

    @Override
    public CompositePrefixingFixedClientIdFilter createFilter(FilterCreationContext context,
                                                              CompositePrefixingFixedClientIdFilterConfig configuration) {
        return new CompositePrefixingFixedClientIdFilter(configuration);
    }
}
