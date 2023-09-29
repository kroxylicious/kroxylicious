/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.internal.filter.FetchResponseTransformationFilter.FetchResponseTransformationConfig;

public class FetchResponseTransformationFilterFactory
        extends FilterFactory<FetchResponseTransformationFilter, FetchResponseTransformationConfig> {

    public FetchResponseTransformationFilterFactory() {
        super(FetchResponseTransformationConfig.class, FetchResponseTransformationFilter.class);
    }

    @Override
    public FetchResponseTransformationFilter createFilter(FilterCreationContext context,
                                                          FetchResponseTransformationConfig configuration) {
        return new FetchResponseTransformationFilter(configuration);
    }
}
