/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.FilterFactory;

/**
 * A filter factory instance and a config object.
 * @param filterFactory
 * @param config
 */
record BoundFilter<C, F extends Filter>(FilterFactory<F, C> filterFactory, C config) {
    public F createFilter(FilterCreationContext ctx) {
        return filterFactory.createFilter(ctx, config());
    }
}
