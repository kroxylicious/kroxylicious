/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterConstructContext;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.internal.filter.FetchResponseTransformationFilter.FetchResponseTransformationConfig;
import io.kroxylicious.proxy.internal.filter.ProduceRequestTransformationFilter.ProduceRequestTransformationConfig;
import io.kroxylicious.proxy.service.BaseContributor;

public class BuiltinFilterContributor extends BaseContributor<Filter, FilterConstructContext> implements FilterContributor {

    public static final BaseContributorBuilder<Filter, FilterConstructContext> FILTERS = BaseContributor.<Filter, FilterConstructContext> builder()
            .add("ProduceRequestTransformation", ProduceRequestTransformationConfig.class, ProduceRequestTransformationFilter::new)
            .add("FetchResponseTransformation", FetchResponseTransformationConfig.class, FetchResponseTransformationFilter::new);

    public BuiltinFilterContributor() {
        super(FILTERS);
    }
}
