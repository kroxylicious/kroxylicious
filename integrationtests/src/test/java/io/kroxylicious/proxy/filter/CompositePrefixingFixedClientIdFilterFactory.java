/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.filter.CompositePrefixingFixedClientIdFilter.CompositePrefixingFixedClientIdFilterConfig;

public class CompositePrefixingFixedClientIdFilterFactory
        extends FilterFactory<CompositePrefixingFixedClientIdFilter, CompositePrefixingFixedClientIdFilterConfig> {

    public CompositePrefixingFixedClientIdFilterFactory() {
        super(CompositePrefixingFixedClientIdFilterConfig.class, CompositePrefixingFixedClientIdFilter.class);
    }

    @Override
    public CompositePrefixingFixedClientIdFilter createFilter(FilterCreationContext context,
                                                              CompositePrefixingFixedClientIdFilterConfig configuration) {
        return new CompositePrefixingFixedClientIdFilter(configuration);
    }
}
