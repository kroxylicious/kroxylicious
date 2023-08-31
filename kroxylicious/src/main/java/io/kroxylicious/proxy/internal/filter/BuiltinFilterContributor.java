/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.internal.filter.FetchResponseTransformationFilter.FetchResponseTransformationConfig;
import io.kroxylicious.proxy.internal.filter.ProduceRequestTransformationFilter.ProduceRequestTransformationConfig;
import io.kroxylicious.proxy.service.BaseContributor;

public class BuiltinFilterContributor extends BaseContributor<Filter> implements FilterContributor {

    public static final BaseContributorBuilder<Filter> FILTERS = BaseContributor.<Filter> builder()
            .add("ProduceRequestTransformation", ProduceRequestTransformationConfig.class, ProduceRequestTransformationFilter::new)
            .add("FetchResponseTransformation", FetchResponseTransformationConfig.class, FetchResponseTransformationFilter::new);

    public BuiltinFilterContributor() {
        super(FILTERS);
    }
}
