/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.filter.FixedClientIdFilter.FixedClientIdFilterConfig;

public class FixedClientIdFilterFactory implements FilterFactory<FixedClientIdFilter, FixedClientIdFilterConfig> {

    @Override
    public Class<FixedClientIdFilter> filterType() {
        return FixedClientIdFilter.class;
    }

    @Override
    public Class<FixedClientIdFilterConfig> configType() {
        return FixedClientIdFilterConfig.class;
    }

    @Override
    public FixedClientIdFilter createFilter(FilterCreationContext context, FixedClientIdFilterConfig configuration) {
        return new FixedClientIdFilter(configuration);
    }
}
