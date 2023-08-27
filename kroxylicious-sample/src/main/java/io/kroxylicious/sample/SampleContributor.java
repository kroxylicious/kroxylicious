/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sample;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.service.BaseContributor;
import io.kroxylicious.proxy.service.Context;
import io.kroxylicious.sample.config.SampleFilterConfig;

public class SampleContributor extends BaseContributor<Filter, Context> implements FilterContributor {

    public static final String SAMPLE_FETCH = "SampleFetchResponse";
    public static final String SAMPLE_PRODUCE = "SampleProduceRequest";
    public static final BaseContributorBuilder<Filter> FILTERS = BaseContributor.<Filter> builder()
            .add(SAMPLE_FETCH, SampleFilterConfig.class, SampleFetchResponseFilter::new)
            .add(SAMPLE_PRODUCE, SampleFilterConfig.class, SampleProduceRequestFilter::new);

    public SampleContributor() {
        super(FILTERS);
    }
}
