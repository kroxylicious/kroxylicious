/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.internal.filter.ProduceRequestTransformationFilter.ProduceRequestTransformationConfig;

public class ProduceRequestTransformationFilterFactory
        extends FilterFactory<ProduceRequestTransformationFilter, ProduceRequestTransformationConfig> {

    public ProduceRequestTransformationFilterFactory() {
        super(ProduceRequestTransformationConfig.class, ProduceRequestTransformationFilter.class);
    }

    @Override
    public ProduceRequestTransformationFilter createFilter(FilterCreationContext context,
                                                           ProduceRequestTransformationConfig configuration) {
        return new ProduceRequestTransformationFilter(configuration);
    }
}
