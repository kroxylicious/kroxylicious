/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sample;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterConstructContext;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.service.BaseContributor;
import io.kroxylicious.sample.config.SampleFilterConfig;

public class SampleContributor extends BaseContributor<Filter, FilterConstructContext> implements FilterContributor {

    public static final String SAMPLE_FETCH = "SampleFetchResponse";
    public static final String SAMPLE_PRODUCE = "SampleProduceRequest";
    public static final BaseContributorBuilder<Filter, FilterConstructContext> FILTERS = BaseContributor.<Filter, FilterConstructContext> builder()
            .add(SAMPLE_FETCH, SampleFilterConfig.class, SampleFetchResponseFilter::new)
            .add(SAMPLE_PRODUCE, SampleFilterConfig.class, SampleProduceRequestFilter::new);

    public SampleContributor() {
        super(FILTERS);
    }
}
