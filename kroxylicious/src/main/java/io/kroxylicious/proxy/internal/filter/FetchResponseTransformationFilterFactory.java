/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.internal.filter.FetchResponseTransformationFilter.FetchResponseTransformationConfig;

import edu.umd.cs.findbugs.annotations.NonNull;

public class FetchResponseTransformationFilterFactory
        implements FilterFactory<FetchResponseTransformationFilter, FetchResponseTransformationConfig> {

    @NonNull
    @Override
    public Class<FetchResponseTransformationFilter> filterType() {
        return FetchResponseTransformationFilter.class;
    }

    @Override
    public Class<FetchResponseTransformationConfig> configType() {
        return FetchResponseTransformationConfig.class;
    }

    @Override
    public FetchResponseTransformationFilter createFilter(FilterCreationContext context,
                                                          FetchResponseTransformationConfig configuration) {
        return new FetchResponseTransformationFilter(configuration);
    }
}
