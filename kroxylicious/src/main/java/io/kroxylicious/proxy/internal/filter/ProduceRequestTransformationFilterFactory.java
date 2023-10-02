/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.internal.filter.ProduceRequestTransformationFilter.ProduceRequestTransformationConfig;

import edu.umd.cs.findbugs.annotations.NonNull;

public class ProduceRequestTransformationFilterFactory
        implements FilterFactory<ProduceRequestTransformationFilter, ProduceRequestTransformationConfig> {

    @NonNull
    @Override
    public Class<ProduceRequestTransformationFilter> filterType() {
        return ProduceRequestTransformationFilter.class;
    }

    @Override
    public Class<ProduceRequestTransformationConfig> configType() {
        return ProduceRequestTransformationConfig.class;
    }

    @Override
    public ProduceRequestTransformationFilter createFilter(FilterCreationContext context,
                                                           ProduceRequestTransformationConfig configuration) {
        return new ProduceRequestTransformationFilter(configuration);
    }
}
